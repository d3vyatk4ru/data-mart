import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, when, lit, regexp_replace, lower, concat, sum, explode, split}

object DataMart {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.cassandra.connection.host", "10.0.0.31")
      .config("spark.cassandra.connection.port", "9042")
      .appName("DataMart_9kin")
      .getOrCreate()

    // Читаем данные из Сassandra, преобразуем возраст
    val clients: DataFrame = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()

    // читаем данные из HDFS
    val weblogs = spark.read
      .format("org.apache.spark.sql.json")
      .load("hdfs:///labs/laba03/weblogs.json")

    // Читаем данные из elasticsearch, создаем столбцы shop_*
    val visits: DataFrame = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.read.metadata" -> "true",
        "es.nodes.wan.only" -> "true",
        "es.port" -> "9200",
        "es.nodes" -> "10.0.0.31",
        "es.net.ssl" -> "false"))
      .load("visits")

    // читаем данные из postgresql
    val domain_cats: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
      .option("driver", "org.postgresql.Driver")
      .option("user", "daniil_devyatkin")
      .option("password", "8I2rBCF2")
      .option("dbtable", "domain_cats")
      .load()

      /* Начинаем сбор витрины */

    val categoryUidCount: DataFrame = visits
        .groupBy(col("uid"), col("category"))
        .count()

    // чистим данные
    val categoryUidCountPrepaired: DataFrame = categoryUidCount
      .withColumn(
        "category",
        regexp_replace(
          regexp_replace(
            lower(concat(lit("shop_"), col("category"))),
            "-", "_"), " ", "_"))

    // вытягиванием столбец категории в колонки
    val uidShopCategory: DataFrame = categoryUidCountPrepaired
      .groupBy("uid")
      .pivot("category")
      .agg(sum("count"))
      .na.fill(value = 0)

    // фильтруем по возрасту и записываем уникальные знаечния интервала
    val cond = when(col("age") >= 18 && col("age") <= 24, "18-24").
      when(col("age") >= 25 && col("age") <= 34, "25-34").
      when(col("age") >= 35 && col("age") <= 44, "35-44").
      when(col("age") >= 45 && col("age") <= 54, "45-54").
      otherwise(">=55")

    val clienstGrouped: DataFrame = clients
      .withColumn("age_cat", cond)

    // добавляем к названиям приставку
    val preparedDomainCats: DataFrame = domain_cats.
      withColumn("category", concat(lit("web_"), col("category")))

    // записываем значения массива из одной строки в несколько
    val explodeVisits: DataFrame = weblogs
      .select(col("uid"), explode(col("visits")).alias("visits"))

    // создаем временную таблицу
    explodeVisits.createOrReplaceTempView("tempTableVisits")

    // приводим домены к нужному виду
    val domainUid: DataFrame = spark.sql("select uid, visits.url from tempTableVisits")
      .withColumn("url", split(col("url"), "/").getItem(2))
      .withColumn("url", regexp_replace(col("url"), "www.", ""))

    // удаляем временную таблицу
    spark.catalog.dropTempView("tempTableVisits")

    val uidDomainCategory: DataFrame = domainUid
      .join(
        preparedDomainCats,
        preparedDomainCats("domain") === domainUid("url"),
        "inner")
      .groupBy("uid", "category")
      .count()
      .groupBy("uid")
      .pivot("category")
      .agg(sum("count"))
      .na.fill(value = 0)

    // финальная витрина
    val final_mart = clienstGrouped
      .join(uidShopCategory, clients("uid") === uidShopCategory("uid"), "left")
      .join(uidDomainCategory, clients("uid") === uidDomainCategory("uid"), "left")
      .na.fill(value = 0)
      .drop(uidShopCategory("uid"))
      .drop(uidDomainCategory("uid"))

    final_mart.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/daniil_devyatkin")
      .option("dbtable", "clients")
      .option("user", "daniil_devyatkin")
      .option("password", "8I2rBCF2")
      .mode("overwrite")
      .option("driver", "org.postgresql.Driver")
      .save()
  }
}