package com.Streaming_Blacklist

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object socketTextStream {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[40]").setAppName("Network")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val lines = ssc.socketTextStream("localhost",8000)

    val blacks = List("zs", "ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x=>(x,true))



    val click = lines.map(x =>(x.split(",")(1),x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD).filter(x=> x._2._2.getOrElse(false) != true)
        .map(x=>x._2._1)
    })

    click.print()

    ssc.start()
    ssc.awaitTermination()
  }


}
