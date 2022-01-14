package ru.yandex.spark.e2e.check

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.table.OptimizeMode
import ru.yandex.yt.ytclient.proxy.CompoundClient

import java.nio.charset.StandardCharsets

sealed abstract class CheckResult(val name: String) {
  def flagPath(path: Path): Path = new Path(path, s"_${name.toUpperCase()}")

  def write(path: Path)(implicit fs: FileSystem): Unit = {
    fs.delete(path, true)
    fs.mkdirs(path)
    fs.createNewFile(flagPath(path))
  }
}

object CheckResult {
  case object Ok extends CheckResult("ok")

  case class WrongData(df: Option[DataFrame]) extends CheckResult("wrong_answer") {
    override def write(path: Path)(implicit fs: FileSystem): Unit = {
      fs.delete(path, true)
      import ru.yandex.spark.yt._
      df.foreach(_.write
        .mode(SaveMode.Overwrite)
        .optimizeFor(OptimizeMode.Scan)
        .yt(flagPath(path).toString))
    }
  }

  case object Empty extends CheckResult("empty")

  case class WrongSchema(errors: Map[String, String]) extends CheckResult("wrong_schema") {
    override def write(path: Path)(implicit fs: FileSystem): Unit = {
      fs.delete(path, true)
      fs.mkdirs(path)
      val out = fs.create(flagPath(path))
      try {
        out.write(errors.map{case (k, v) => s"$k->$v"}.mkString(";").getBytes(StandardCharsets.UTF_8))
      } finally {
        out.close()
      }
    }
  }

  case class UnexpectedError(message: String) extends CheckResult(s"unexpected_error") {
    override def write(path: Path)(implicit fs: FileSystem): Unit = {
      fs.delete(path, true)
      fs.mkdirs(path)
      val out = fs.create(flagPath(path))
      try {
        out.write(message.getBytes(StandardCharsets.UTF_8))
      } finally {
        out.close()
      }
    }
  }

  case object Unknown extends CheckResult("unknown")

  def read(path: String)(implicit yt: CompoundClient,
                       spark: SparkSession): CheckResult = {
    YtWrapper.listDir(path).toList match {
      case Nil => Unknown
      case s :: Nil =>
        s match {
          case "_OK" => CheckResult.Ok
          case "_WRONG_ANSWER" =>
            import ru.yandex.spark.yt._
            CheckResult.WrongData(Some(spark.read.yt(s"$path/$s")))
          case "_EMPTY" => CheckResult.Empty
          case "_WRONG_SCHEMA" => CheckResult.WrongSchema(Map.empty)
          case "_UNEXPECTED_ERROR" => CheckResult.UnexpectedError("")
          case _ => Unknown
        }
      case _ => Unknown
    }
  }

  def readWithoutDetails(path: String)
                        (implicit yt: CompoundClient): CheckResult = {
    YtWrapper.listDir(path).toList match {
      case Nil => Unknown
      case s :: Nil =>
        s match {
          case "_OK" => CheckResult.Ok
          case "_WRONG_ANSWER" => CheckResult.WrongData(None)
          case "_EMPTY" => CheckResult.Empty
          case "_WRONG_SCHEMA" => CheckResult.WrongSchema(Map.empty)
          case "_UNEXPECTED_ERROR" => CheckResult.UnexpectedError("")
          case _ => Unknown
        }
      case _ => Unknown
    }
  }
}


