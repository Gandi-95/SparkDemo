package spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1.需求：有一个微博网站，下面有很多栏目，每个栏目下面都有几千万用户，每个用户会有很多的粉丝，
  *         要求取出各栏目粉丝量最多的用户TopN。【可用TreeMap实现，专栏：feature， 粉丝：fan】
  *         体育 user01 user04 user05 user08 user09 user10
  */
object WeiboStatistics {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WeiboStatistics").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val logRDD = sc.textFile("D:\\IdeaProjects\\SparkDemo\\src\\main\\resources\\WeiboStatisticsLog")
      .map(line => line.split(" "))
      .map(x => ((x(0),x(1)),x.length-2))
      .sortBy(_._2,false)     //按照value进行降序排序
      .groupBy(_._1._1)
      .foreach(println)
  }

}
