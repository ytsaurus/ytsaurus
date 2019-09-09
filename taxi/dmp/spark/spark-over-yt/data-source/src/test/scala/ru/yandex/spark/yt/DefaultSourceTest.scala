package ru.yandex.spark.yt

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Encoders, SaveMode}
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.functions._

class DefaultSourceTest extends FlatSpec with Matchers with LocalSpark {

  behavior of "DefaultSourceTest"

  import DefaultRpcCredentials._

  it should "createRelation" ignore {
    val df = spark.read
      .option("proxy", "hume")
      .option("user", user)
      .option("token", token)
      .option("partitions", 3)
      .yt("//tmp/sashbel")

    df.show()
  }

  it should "show" ignore {
    val df = spark.read.option("proxy", "hume").option("partitions", 3).yt("//tmp/sashbel")

    df.show()
  }

  it should "show unique_drivers" ignore {
    val df = spark.read
      .option("proxy", "hume")
      .option("partitions", 10000)
      .yt("//home/taxi-dwh/stg/mdb/unique_drivers/unique_drivers")

    df.show()
  }

  it should "show unique_drivers with hints" in {
    val df = spark.read
      .option("proxy", "hume")
      .option("partitions", 10000)
      .schemaHint2(
        "licenses" -> ArrayType(StringType)
      )
      .format("yt")
      .load("yt://hume/home/taxi-dwh/stg/mdb/unique_drivers/unique_drivers")

    df.printSchema()

    df.show()
  }

  it should "show snp_acc_driver_license" in {
    val df = spark.read.hume.partitions(100).yt("//home/taxi-dwh/dds/snp_acc_driver_license")
    df.printSchema()
    df.show()
  }

  it should "join snp_acc_driver_license with unique_drivers" ignore {
    val driverLicense = spark.read
      .option("proxy", "hume")
      .option("partitions", 10000)
      .yt("//home/taxi-dwh/dds/snp_acc_driver_license")

    val uniqueDrivers = spark.read
      .option("proxy", "hume")
      .option("partitions", 10000)
      .schemaHint(
        StructField("licenses", ArrayType(StringType))
      )
      .yt("//home/taxi-dwh/stg/mdb/unique_drivers/unique_drivers")
      .withColumn("driver_license", explode(col("licenses")))

    driverLicense.join(uniqueDrivers, Seq("driver_license")).show()
  }

  it should "group by" ignore {
    import spark.implicits._

    val ds = spark.read
      .option("proxy", "hume")
      .option("partitions", 10000)
      .yt("//home/taxi-dwh/dds/snp_acc_driver_license")
      .withColumnRenamed("id", "unique_driver_id")
      .selectAs[Driver]

    ds.printSchema()

    ds
      .groupByKey(_.unique_driver_id)
      .mapGroups { case (id, rows) =>
        val collected = rows.toList
        val firstSuccess = collected.minBy(_.first_success_order_created)
        val firstOrder = collected.minBy(_.first_order_created)
        val lastSuccess = collected.minBy(_.last_success_order_created)
        val lastOrder = collected.minBy(_.last_order_created)

        Result(
          unique_driver_id = id,
          first_success_order_created = firstSuccess.first_success_order_created,
          first_success_order_id = firstSuccess.first_success_order_id,
          first_success_order_city = firstSuccess.first_success_order_city,
          first_order_created = firstOrder.first_order_created,
          first_order_id = firstOrder.first_order_id,
          first_order_city = firstOrder.first_order_city,
          last_success_order_created = lastSuccess.last_success_order_created,
          last_success_order_id = lastSuccess.last_success_order_id,
          last_success_order_city = lastSuccess.last_success_order_city,
          last_success_order_driver_id = lastSuccess.last_success_order_driver_id,
          last_order_created = lastOrder.last_order_created,
          last_order_id = lastOrder.last_order_id,
          last_order_city = lastOrder.last_order_city
        )
      }
  }

  it should "write df" in {
    import spark.implicits._
    Seq(1L, 2L, 3L).toDF().repartition(3).write.mode(SaveMode.Append).option("proxy", "hume").option("partitions", 1).yt("//home/sashbel/data/test")
  }

  it should "write df with nulls" ignore {
    import spark.implicits._
    Seq(Some(1L), Some(2L), Some(3L), None).toDF().repartition(3)
      .write.mode(SaveMode.Append).option("proxy", "hume").option("partitions", 1).yt("//tmp/sashbel_test")
  }

  it should "write df with array" in {
    import spark.implicits._
//    Seq(Seq(1, 2, 3)).toDF()
//      .repartition(3)
//      .write.mode(SaveMode.Overwrite).hume
//      .yt("//home/sashbel/data/test")

    spark.read.hume.partitions(1)
      .schemaHint("value" -> ArrayType(LongType))
      .yt("//home/sashbel/data/test")
      .show(truncate=false)
  }

  it should "overwrite df" ignore {
    import spark.implicits._

    Seq(Some(1L), Some(2L), Some(3L), None).maxBy(_.map(-_))

    Seq(Some("1"), Some("2"), Some("3"), None).max(scala.math.Ordering.Option(Ordering[String].reverse))

    Seq(Some(1L), Some(2L), Some(3L), None).toDF().repartition(3)
      .write.mode(SaveMode.Overwrite).hume.yt("//tmp/sashbel_test")
  }

  it should "parse array" ignore {
    val df = spark.read
      .hume.partitions(1)
      .schemaHint(StructField("licenses", ArrayType(StringType)))
      .yt("//tmp/sashbel/test/array_parsing")
    df.show()
    df.select("id").show()
    df.count()
  }

  it should "count df" in {
    val df = spark.read
      .hume.partitions(100)
      .yt("//home/taxi-dwh/dds/snp_acc_driver_license")

    println(df.count())
  }

  it should "read parquet" in {
    import spark.implicits._
    println(spark.read.parquet("/tmp/test2").show())
  }

  it should "read yt as file" in {
    import spark.implicits._
    val df = spark.read.hume.partitions(100).format("yt").load("yt://hume/home/taxi-dwh/dds/snp_acc_driver_license")
    df.printSchema()
    df.show()
    println(df.count())
    val start = System.currentTimeMillis()
    println(df.filter('driver_license === "").count())
    val end = System.currentTimeMillis()
    println((end - start) / 1000)
  }

  it should "read complex types" in {
    import spark.implicits._
    import ru.yandex.spark.yt.utils.DateTimeUtils._

    val users = spark.read.hume.partitions(100)
      .schemaHint(
        "doc" -> StructType(Seq(
          StructField("_id", StringType),
          StructField("created", StringType),
          StructField("updated", StringType),
          StructField("device_id", StringType),
          StructField("phone_id", StringType),
          StructField("yandex_uid", StringType),
          StructField("yandex_uuid", StringType),
          StructField("application", StringType),
          StructField("token_only", BooleanType),
          StructField("authorized", BooleanType),
          StructField("has_ya_plus", BooleanType),
          StructField("yandex_staff", BooleanType),
          StructField("banners_enabled", ArrayType(StringType)),
          StructField("banners_seen", ArrayType(StringType))
        ))
      )
      .yt("//home/taxi-dwh/raw/mdb/users/users")

    users.select(
      $"doc._id" as "user_id",
      formatDatetime($"doc.created") as "utc_created_dttm",
      formatDatetime($"doc.updated") as "utc_updated_dttm",
      emptyStringToNull($"doc.device_id") as "device_id",
      $"doc.phone_id" as "user_phone_id",
      emptyStringToNull($"doc.yandex_uid") as "yandex_uid",
      emptyStringToNull($"doc.yandex_uuid") as "yandex_uuid",
      emptyStringToNull($"doc.application") as "application_platform",
      $"doc.token_only" as "token_only_flg",
      $"doc.authorized" as "authorized_flg",
      $"doc.has_ya_plus" as "has_ya_plus_flg",
      $"doc.yandex_staff" as "yandex_staff_flg",
      $"doc.banners_enabled" as "banners_enabled_list",
      $"doc.banners_seen" as "banners_seen_list"
    ).na.fill(value = false, Seq("authorized_flg", "has_ya_plus_flg")).show(10, truncate = false)
  }

  it should "collect top" in {
    import spark.implicits._

    val df = Seq(
      ("1", "1", "1", "a"),
      ("1", "1", "2", "b"),
      ("1", "1", "3", "c"),
      ("1", "2", "1", "d"),
      ("1", "2", "2", "e"),
      ("2", "3", "1", "f")
    ).
      toDF("user_phone", "brand", "moscow_event_dttm", "target")

    df.
      repartition(1, 'user_phone, 'brand).
      sortWithinPartitions("user_phone", "brand", "moscow_event_dttm").
      mapPartitions{rows =>
        var currentKey = Option.empty[(String, String)]
        rows.flatMap { row =>
          val key = (row.getAs[String]("user_phone"), row.getAs[String]("brand"))
          if (!currentKey.contains(key)) {
            currentKey = Some(key)
            Some(row)
          } else {
            None
          }
        }
      }(RowEncoder(df.schema)).show()
  }

  it should "convert struct to binary" in {
    import spark.implicits._

    Seq(
      (1, 2, 3),
      (4, 5, 6)
    )
      .toDF("a", "b", "c")
      .withColumn("d", map(lit("a"),'a, lit("b"), 'b, lit("c"), 'c))
      .withColumn("e", lit("e"))
      .write.hume.mode(SaveMode.Overwrite).yt("//home/sashbel/data/test")

    spark.read.hume.partitions(1)
      .schemaHint(
        "d" -> StructType(Seq(
          StructField("a", LongType),
          StructField("b", LongType),
          StructField("c", LongType)
        ))
      )
      .yt("//home/sashbel/data/test")
      .show(truncate = false)
  }

  it should "optimize for scan" in {
    import spark.implicits._
    Seq(
      (1, 2, 3),
      (4, 5, 6)
    )
      .toDF("a", "b", "c")
      .write.hume
      .optimizeFor(OptimizeMode.Scan)
      .mode(SaveMode.Overwrite)
      .yt("//home/sashbel/data/test")
  }

  it should "read map" in {
    val df = spark.read.hume.partitions(100).schemaHint(
      "info" -> MapType(StringType, StringType)
    ).yt("//home/taxi-dwh/dds/fct_ca_performance_event/2019-08-01")

    df.show()
  }

  it should "show nulls" in {
    spark.read.hume.yt("//home/sashbel/data/test_project_input").show
  }
}

case class Driver(unique_driver_id: String,
                  first_success_order_created: String,
                  first_success_order_id: String,
                  first_success_order_city: String,
                  first_order_created: String,
                  first_order_id: String,
                  first_order_city: String,
                  last_success_order_created: String,
                  last_success_order_id: String,
                  last_success_order_city: String,
                  last_success_order_driver_id: Option[String],
                  last_order_created: String,
                  last_order_id: String,
                  last_order_city: String)

case class Result(unique_driver_id: String,
                  first_success_order_created: String,
                  first_success_order_id: String,
                  first_success_order_city: String,
                  first_order_created: String,
                  first_order_id: String,
                  first_order_city: String,
                  last_success_order_created: String,
                  last_success_order_id: String,
                  last_success_order_city: String,
                  last_success_order_driver_id: Option[String],
                  last_order_created: String,
                  last_order_id: String,
                  last_order_city: String)
