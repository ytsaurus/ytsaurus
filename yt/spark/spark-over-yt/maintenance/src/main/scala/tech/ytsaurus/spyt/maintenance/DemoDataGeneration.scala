package tech.ytsaurus.spyt.maintenance

import org.apache.spark.sql.SaveMode
import org.threeten.extra.Seconds
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode

import java.time.LocalDateTime
import java.util.UUID
import scala.util.Random

object DemoDataGeneration extends SparkApp {
  import spark.implicits._

  def generateUsers(): Unit = {
    val cities = Seq("London", "Paris", "Berlin", "Yerevan", "Tbilisi", "Istanbul")
    val users = (1 to 1000).toDS.flatMap { i =>
      val r = Random
      (1 to 10000).iterator.map { j =>
        val id = UUID.randomUUID().toString
        val city = cities(r.nextInt(cities.length).min(r.nextInt(cities.length)))
        val phoneNumber = s"+${(1 to 10).map(_ => r.nextInt(10)).mkString("")}"
        val age = 18 + 35 + (r.nextGaussian.min(1.0).max(-1.0) * 35).toInt
        val isMarried = r.nextInt(100) > 70
        (id, city, phoneNumber, age, isMarried)
      }
    }.select('_1 as "id", '_2 as "city", '_3 as "phone_number", '_4 as "age", '_5 as "is_married")

    users.write
      .mode(SaveMode.Overwrite)
      .optimizeFor(OptimizeMode.Scan)
      .yt("//home/sashbel/data/yt_demo/user")
  }

  def generateLog(): Unit = {
    val users = spark.read.yt("//home/sashbel/data/yt_demo/user").select('id as "user_id").as[String].take(3000000)
    val eventTypes = Seq("click", "keypress", "close", "open", "accept", "decline", "timeout", "other")
    val deviceTypes = Seq("tablet", "smartphone", "other")
    val userToDevice = users.drop(500000).map(id => id -> (UUID.randomUUID().toString, UUID.randomUUID().toString)).toMap

    val log = (1 to 200).toDS.flatMap { i =>
      val start = LocalDateTime.parse("2022-07-01T00:00:00")
      val end = LocalDateTime.parse("2022-07-08T00:00:00")
      val secondsBetween = Seconds.between(start, end).getAmount
      val r = Random
      (1 to 1000000).iterator.map { j =>
        val dttm = start.plus(Seconds.of(r.nextInt(secondsBetween))).toString
        val user = users(r.nextInt(users.length))
        val eventType = eventTypes(r.nextInt(eventTypes.length))
        val deviceId = userToDevice.get(user).map { case (id1, id2) => if (r.nextInt(10) > 8) id2 else id1}
        val deviceType = deviceId.map(id => deviceTypes(id.hashCode().abs % deviceTypes.length))
        (dttm, user, eventType, deviceId, deviceType)
      }
    }.select('_1 as "dttm", '_2 as "user_id", '_3 as "event_type", '_4 as "device_id", '_5 as "device_type")

    log
      .sort('dttm)
      .write
      .mode(SaveMode.Overwrite)
      .optimizeFor(OptimizeMode.Scan)
      .sortedBy("dttm")
      .yt("//home/sashbel/data/yt_demo/log")

  }

  case class Info(comment: String, is_final: Boolean, value: Double)

  def generateDynTable(): Unit = {
    val statuses = Seq("waiting", "ready", "accepted", "created", "success", "fail")
    val text =
      """The crawlway was Yossarian's lifeline to outside from a plane about to fall, but Yossarian swore at
        |it with seething antagonism, reviled it as an obstacle put there by providence as part of the plot that
        |would destroy him. There was room for an additional escape hatch right there in the nose of a B-25,
        |but there was no escape hatch. Instead there was the crawlway, and since the mess on the mission
        |over Avignon he had learned to detest every mammoth inch of it, for it slung him seconds and
        |seconds away from his parachute, which was too bulky to be taken up front with him, and seconds
        |and seconds more after that away from the escape hatch on the floor between the rear of the
        |elevated flight deck and the feet of the faceless top turret gunner mounted high above. Yossarian
        |longed to be where Aarfy could be once Yossarian had chased him back from the nose; Yossarian
        |longed to sit on the floor in a huddled ball right on top of the escape hatch inside a sheltering igloo
        |of extra flak suits that he would have been happy to carry along with him, his parachute already
        |hooked to his harness where it belonged, one fist clenching the red-handled rip cord, one fist
        |gripping the emergency hatch release that would spill him earthward into the air at the first dreadful
        |squeal of destruction. That was where he wanted to be if he had to be there at all, instead of hung
        |out there in front like some goddam cantilevered goldfish in some goddam cantilevered goldfish
        |bowl while the goddam foul black tiers of flak were bursting and booming and billowing all around
        |and above and below him in a climbing, cracking, staggered, banging, phantasmagorical,
        |cosmological wickedness that jarred and tossed and shivered, clattered and pierced, and threatened
        |to annihilate them all in one splinter of a second in one vast flash of fire.""".stripMargin.replace("\n", " ").split(" ")

    val processStatus = (1 to 100).toDS().flatMap{ i =>
      val start = LocalDateTime.parse("2022-07-01T00:00:00")
      val end = LocalDateTime.parse("2022-07-08T00:00:00")
      val secondsBetween = Seconds.between(start, end).getAmount
      val r = Random
      (1 to 1000).map { j =>
        val id = UUID.randomUUID().toString
        val dttm = start.plus(Seconds.of(r.nextInt(secondsBetween))).toString
        val status = statuses(r.nextInt(statuses.length))
        val sliceI = r.nextInt(text.length)
        val sliceJ = r.nextInt(text.length)
        val comment = text.slice(sliceI.min(sliceJ), sliceI.max(sliceJ)).mkString(" ")
        val isFinal = status == "success" || status == "fail"
        val info = Info(comment, isFinal, r.nextGaussian())
        (id, dttm, status, info)
      }
    }.select('_1 as "id", '_2 as "updated_dttm", '_3 as "status", '_4 as "info")

    processStatus
      .sort('id)
      .write
      .mode(SaveMode.Overwrite)
      .optimizeFor(OptimizeMode.Scan)
      .sortedByUniqueKeys("id")
      .yt("//home/sashbel/data/yt_demo/process_status")


  }

}
