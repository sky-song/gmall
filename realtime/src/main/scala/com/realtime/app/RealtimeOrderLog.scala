package com.realtime.app

import com.alibaba.fastjson.JSON
import com.constant.GmallConstant
import com.realtime.bean.OrderInfo
import com.realtime.util.MyKafkaUtil
import com.util.MyESUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealtimeOrderLog {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("orderlog").setMaster("local[2]")

        val ssc = new StreamingContext(conf,Seconds(5))

        val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.TOPIC_ORDER,ssc)

        //将新增订单保存到ES中
        val orderInfoDStream = recordDStream.map(_.value()).map(jsonString => {
            val orderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
            val dateArray = orderInfo.createTime.split(" ")
            val dateString = dateArray(0)

            val timeArray = dateArray(1).split(":")
            val hour = timeArray(0)
            val minute = timeArray(1)

            orderInfo.createDate = dateString
            orderInfo.createHour = hour
            orderInfo.createHourMinute = hour + ":" + minute

            orderInfo
        })

        //将新增订单保存到ES中
        orderInfoDStream.foreachRDD{rdd=>
            rdd.foreachPartition{orderItr=>
                MyESUtil.executeIndexBulk(GmallConstant.ES_INDEX_ORDER,orderItr.toList,"")
            }
        }


    }

}
