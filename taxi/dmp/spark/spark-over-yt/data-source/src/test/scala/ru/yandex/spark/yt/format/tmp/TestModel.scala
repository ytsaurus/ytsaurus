package ru.yandex.spark.yt.format.tmp

case class A(field1: Int, field2: Option[String])

case class B(a: Option[String])

case class C(a: Map[String, Option[Long]], b: B, c: Seq[Option[Boolean]], d: Option[Double])

case class Test(f1: Map[String, Option[Map[String, Option[Long]]]],
                f2: Map[String, Option[B]],
                f3: Map[String, Option[Seq[Option[Double]]]],
                f4: Map[String, Option[Boolean]],
                f5: C,
                f6: Seq[Option[Map[String, Option[Long]]]],
                f7: Seq[Option[B]],
                f8: Seq[Option[Seq[Option[Long]]]],
                f9: Seq[Option[Double]],
                f10: Map[Long, Option[Map[String, Option[Boolean]]]],
                f11: Map[Option[Map[String, Option[Boolean]]], Long])

case class TestSmall(f1: Map[String, Option[Map[String, Option[Long]]]],
                     f4: Map[String, Option[Boolean]],
                     f7: Seq[Option[B]],
                     f10: Map[Long, Option[Map[String, Option[Boolean]]]],
                     f11: Map[Option[Map[String, Option[Boolean]]], Long])
