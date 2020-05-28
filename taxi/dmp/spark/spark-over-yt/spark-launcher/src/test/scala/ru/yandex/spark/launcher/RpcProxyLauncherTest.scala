package ru.yandex.spark.launcher

import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder

class RpcProxyLauncherTest extends FlatSpec with Matchers {

  behavior of "RpcProxyLauncherTest"

  it should "update" in {
    import scala.collection.JavaConverters._

    val node = new YTreeBuilder()
      .beginMap()
      .key("key1").value("before")
      .key("key2")
      .beginMap().key("a").value(1).key("b").value(2).endMap()
      .key("key3")
      .beginList().value(1).value(2).endList()
      .key("key4").value("before")
      .endMap()
      .build()

    val patch = new YTreeBuilder()
      .beginMap()
      .key("key1").value("after")
      .key("key2")
      .beginMap().key("b").value(33).key("c").value(22).endMap()
      .key("key3")
      .beginList().value(3).value(4).endList()
      .key("key5").value("after")
      .endMap()
      .build()

    val res = RpcProxyLauncher.update(node, patch).asMap.asScala
    res.keys should contain theSameElementsAs Seq("key1", "key2", "key3", "key4", "key5")
    res("key1").stringValue() shouldEqual "after"
    res("key2").asMap().asScala.mapValues(_.longValue()) shouldEqual Map("a" -> 1, "b" -> 33, "c" -> 22)
    res("key3").asList().asScala.map(_.longValue()) should contain theSameElementsInOrderAs Seq(3, 4)
    res("key4").stringValue() shouldEqual "before"
    res("key5").stringValue() shouldEqual "after"
  }

}
