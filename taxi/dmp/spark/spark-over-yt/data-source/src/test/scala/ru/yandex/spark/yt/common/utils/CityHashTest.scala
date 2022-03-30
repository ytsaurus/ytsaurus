package ru.yandex.spark.yt.common.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.yson.UInt64Long
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.common.utils.CityHash.{registerFunction, cityHashUdf}
import ru.yandex.spark.yt.test.{LocalSpark, TmpDir}

class CityHashTest extends FlatSpec with Matchers with LocalSpark with TmpDir {
  behavior of "CityHashTest"

  import spark.implicits._

  it should "compute hash like chyt" in {
    // random uuid
    val df = Seq(
      "bdc3aa61-cf2e-4350-b0ff-9bda0bcba214", "79f9566a-b9fc-44bf-9903-c320f8beacad",
      "9f8ea8a8-97cd-44b0-a661-0453a198e5e7", "3dca3b34-5210-4aaf-82e4-d39a3be94cc4",
      "65d97e4d-21f2-449f-9374-0ca22d86400b", "cee5a7ec-1962-40ed-956d-67b87616a800",
      "9a5088bc-21f3-4e17-b1a4-8877fc2ebc9c", "c3a1d801-35f0-45fb-add6-cdae6b0d4ca2",
      "f9c8c91b-24db-48d9-aaee-32fb3e5cf644", "3535e360-9244-46f0-9e79-c472c8477df9",
      "2e5c9611-a228-4e83-b5ee-5d377ae484e2", "6efca41f-7b13-4f16-b95e-823eb5f68ca2",
      "4c4ff81e-d5c1-4ec5-85e9-17bb5d6bdbab", "7e3368f3-64cf-4afa-ab1d-a37fc49a5fe1",
      "3e250454-5a3d-473f-b49f-6ebf62b9f0a5", "48b89381-4eaa-49d3-b627-088ae376b3b3",
      "3fbeebb5-e13e-412e-8cce-d464428e1c9a", "68ffe6c3-2096-43f4-9ad4-57cd47092fba",
      "ba33e170-32a9-4590-be65-c9224d4c1c16", "4a192a1e-d79e-459b-a247-b99929c3b4d7",
      "88744b37-aab7-4d17-a4cd-e9a52eaf8f03", "3e7db34a-0f80-412d-81ff-ebe465492448",
      "494926cf-ee2a-46ae-a9f0-6b11d413f361", "fc6ff6d8-79de-4a96-aba3-2f55abbddca6",
      "ffc98b47-ae72-46c6-8652-fd1c8e6884d4", "aa981509-53b9-426b-a269-1bdfbfd2a67b",
      "63193047-2ee6-410c-98fc-d7f24f145694", "eff9431a-0fd4-44f7-84a5-0ee65c6a81d4",
      "6b69a55b-3f3a-4fe0-abe9-4da8cde53f3f", "38fe7509-7672-4c86-ad8b-13054c88a60c",
      "9d7a4383-9494-4ce2-9b12-d87d1cd4c00b", "02eb07c5-bb28-4deb-8433-884bb3d39129",
      "5f0e93f9-3e31-450f-b163-f596c745aa53", "f9dafada-47f9-4a31-906d-de0f75690bed",
      "2877626b-582f-4ada-a183-f7d93c490362", "6dd67069-f4ce-408b-b988-b5e10dbfa53a",
      "882e9b21-1618-401c-81d8-84e7d89ce130", "fd35c90a-0689-4323-b926-eba3755eb07a",
      "f6bd9962-9cb9-4ab3-a26b-a4484759872f", "b66a6085-56b0-46d9-8b87-8dd5c98da864",
      "db256541-103c-45ce-bc3b-41a5588a2c65", "d1dd218b-520e-462e-adbb-4f6a3910139b",
      "f4b2fcaa-3a2b-49dc-991a-35116f8d8d40", "58c5ad92-afe8-49db-b20f-583ca117ca9f",
      "a6db1db9-7cb7-4b5c-9726-2a942e48703b", "0dd51b59-3a34-4d8b-b770-7612bb5452ba",
      "5bbd504a-c417-4627-8a21-cd6665055f7d", "ac857af4-3da9-45ae-9328-b5b39fbab997",
      "956bd688-8c88-49b7-86e8-61a74b868773", "7021619d-1478-4bca-843e-e97fd60cc81a")
      .toDF("a").withColumn("a", cityHashUdf(col("a")))
    df.collect() should contain theSameElementsAs Seq(
      "16094386428308033480", "2152752458081453739", "12571475904063481703", "12944456731626643925",
      "3586693558895237584", "7686190105180647590", "7718638987038045705", "7715023131725572359",
      "14739874475610271721", "12831057974395909907", "15694750129478467389", "13994727230999396509",
      "9468289888862630099", "9438656463146536615", "6399325617846954538", "6840380760912913715",
      "17465961849876338623", "919782869518255653", "3584427477876991263", "12287157094841198650",
      "4706721054654947173", "17745803333316422282", "7605823843344251103", "3730632002956440407",
      "2829243476870064917", "3135683394331928866", "8915369294051542697", "14526870060731451714",
      "14362060400980144869", "767782690285220972", "5455038100839917810", "17641663123906998998",
      "3709540782052500272", "8788882035330286843", "1945918383533521853", "14681308769496706898",
      "6824450057341756653", "6269038458128149475", "2627247326823495006", "4510390942835330295",
      "5849874775967885081", "4737746970184778160", "6805158532416092596", "3373989498310217218",
      "14404628845327035445", "12134390995916588070", "7572555071826627415", "12304582579903404117",
      "14116491843822058238", "11893670925323872417"
    ).map(x => Row(UInt64Long(x)))

    // string hash
    val df2 = Seq("hello", "chyt")
      .toDF("a").withColumn("a", cityHashUdf(col("a")))
    df2.collect() should contain theSameElementsAs Seq(
      Row(UInt64Long("2578220239953316063")),
      Row(UInt64Long("14004987693767423897"))
    )
  }

  it should "compute hash like yql" in {
    // test for yandex specific city hashing (> 64 bytes)
    val df = Seq("00000000000000000000000000000000000000000000000000" +
      "00000000000000000000000000000000000000000000000000")
      .toDF("a").withColumn("a", cityHashUdf(col("a")))
    df.collect() should contain theSameElementsAs Seq(
      Row(UInt64Long("4134813464650945862"))
    )

    // string hash
    val df2 = Seq("yql", "+", "spyt")
      .toDF("a").withColumn("a", cityHashUdf(col("a")))
    df2.collect() should contain theSameElementsAs Seq(
      Row(UInt64Long("15201208517282908161")),
      Row(UInt64Long("13742020904102819830")),
      Row(UInt64Long("1553191552111749814"))
    )
  }

  it should "run udf in sql query" in {
    registerFunction(spark)

    // string hash
    val df = spark.sql(s"select cityhash(*) from (values ('0'), ('1'))")
    df.collect() should contain theSameElementsAs Seq(
      Row(UInt64Long("10408321403207385874")),
      Row(UInt64Long("11413460447292444913"))
    )
  }

  it should "fail on unsupported types" in {
    a[UnsupportedOperationException] should be thrownBy {
      Seq(0, 1).toDF("a").withColumn("a", cityHashUdf(col("a")))
        .collect()
    }
  }
}