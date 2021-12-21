package com

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Task1 {
	
case class Schema(txnno :Int,
txndate:String,custno:Int,amount:Double,
category:String,product:String,
city:String,state:String,spendby:String)

  
  def main(args: Array[String]): Unit = {

			val conf =new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().config(conf).getOrCreate()
					import spark.implicits._

					
				val data=sc.textFile("file:///E:/Hadoop data/txns")
				val gymdata=data.filter(x=>x.contains("Gymnastics"))
				val splitdata = gymdata.map(x=> x.split(",")).map(x=>Schema(x(0).toInt,x(1),x(2).toInt,x(3).toDouble,x(4),x(5),x(6),x(7),x(8)))			
				val filterdata = splitdata.filter((x=>x.txnno > 50000))
				filterdata.take(10).foreach(println)
				val df1=filterdata.toDF()
				df1.show(false)
						
						
//			println(spark.sparkContext.applicationId)
//					println(spark.sparkContext.appName)	
//					println(spark.sparkContext.getPersistentRDDs)    
//						println(spark.version)   
						
//						val data=sc.textFile("file:///E:/Hadoop data/txns")
//	val gymdata=data.filter(x=>x.contains("Gymnastics"))
//	val rowdata=gymdata.map(x=>x.split(",")).map(x=>Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
//	val filterdata=rowdata.filter(x=>x(0).toString().toInt>50000)
//	filterdata.take(10)foreach(println)
//	
// To print records greater than length of 100
//
//					val data=sc.textFile("file:///E:/Hadoop data/txns")
//					val lendata = data.take(10).filter(x=>x.length() > 100)
//					lendata.foreach(println(_))


//To get the records who amount is greater than 50 and less than 60.00
//As we have to work on Column of file we need to define a schema 

		
					

	}
}