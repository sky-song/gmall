package com.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.alibaba.fastjson.JSON
import com.constant.GmallConstant
import com.realtime.bean.StartUpLog
import com.realtime.util.MyKafkaUtil
import com.util.MyESUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import scala.collection.mutable.ListBuffer

object RealtimeStartupApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("realtimestartup")
        val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

        val recordStream: InputDStream[ConsumerRecord[String, String]] =
            MyKafkaUtil.getKafkaStream(GmallConstant.TOPIC_STARTUP,ssc)

        //1、将Json字符串转化为样例类对象
        val startUpDStream: DStream[StartUpLog] = recordStream.map(_.value()).map { json =>
            val startUpLog = JSON.parseObject(json, classOf[StartUpLog])
            startUpLog
        }

        //2、对数据过滤，把redis中的数据与当前批次的数据进行对比，过滤掉已经有的数据
        val filterDStream: DStream[StartUpLog] = startUpDStream.transform(rdd => {
            val jedis = new Jedis("hadoop101", 6379)
            val dauSet =
                jedis.smembers("dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
            jedis.close()

            val broadCast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
            val filterRdd: RDD[StartUpLog] = rdd.filter(startup => {
                !broadCast.value.contains(startup.mid)
            })
            filterRdd
        })

        //3、将新的活跃用户的数据保存到redis
        filterDStream.foreachRDD(rdd=>{
            rdd.foreachPartition(start=>{
                val jedis = new Jedis("hadoop101",6379)
                val list = new ListBuffer[StartUpLog]()

                start.foreach(log=>{
                    val formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    val date = new Date(log.ts)
                    val dateTime: String = formatDate.format(date)

                    val strings = dateTime.split(" ")
                    val dateLong = strings(0)
                    val strings1 = strings(1).split(":")
                    val hour = strings1(0)
                    val minute = strings1(1)

                    val key = "dau:"+dateLong
                    jedis.sadd(key,log.mid)

                    //补充一些时间字段，用于es中的时间分析
                    log.logDate = dateLong
                    log.logHours = hour
                    log.logHourMinute = hour + ":" + minute

                    list += log
                })
                jedis.close()
                //3、保存到es中
                MyESUtil.executeIndexBulk(GmallConstant.ES_INDEX_DAU,list.toList,"")

            })
        })

        ssc.start()
        ssc.awaitTermination()

    }

}
