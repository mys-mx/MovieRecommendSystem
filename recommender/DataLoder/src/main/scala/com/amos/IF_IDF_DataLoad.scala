package com.amos

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object IF_IDF_DataLoad {
  // 定义表名和常量
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val CONTENT_MOVIE_RECS = "ContentMovieRecs"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val warehouse = "hdfs://hadoop01:9870/user/hive/warehouse"

    val conf = new SparkConf()
      .setAppName("推荐系统-itemCF")
      .setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //加载数据，并做预处理
    val movieTageDF: DataFrame = spark.read.option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map(
        x => (x.mid, x.name, x.genres.map(c => if (c == '|') ' ' else c))
      )
      .toDF("mid", "name", "genres")
      .cache()

    // 核心部分：用TF-IDF从内容中提取电影特征向量
    // 创建一个分词器，默认按照空格分词


  }

  //定义一个样例类存储mongo的uri和db库名
  case class MongoConfig(uri: String, db: String)

  // 需要的数据源是电影内容信息
  case class Movie(mid: Int, name: String, descri: String, timelong: String,
                   issue: String, shoot: String, language: String,
                   genres: String, actors: String, directors: String)
}
