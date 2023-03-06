package tech.ytsaurus.spyt.wrapper.cypress

import tech.ytsaurus.ysontree.YTreeBuilder
import tech.ytsaurus.ysontree.YTreeNode

object YsonableProduct {
  implicit def ysonWriter[T <: Product]: YsonWriter[T] = new YsonWriter[T] {
    override def toYson(t: T): YTreeNode = {
      t.getClass.getDeclaredFields.foldLeft(new YTreeBuilder().beginMap()){case (builder, f) =>
        f.setAccessible(true)
        toYsonAny(f.get(t), builder.key(f.getName))
      }.endMap().build()
    }

    private def toYsonAny(value: Any, builder: YTreeBuilder): YTreeBuilder = {
      value match {
        case m: Map[String, String] => m.foldLeft(builder.beginMap()){case (b, (k, v)) => b.key(k).value(v)}.endMap()
        case s: Seq[String] => s.foldLeft(builder.beginList()){case (b, v) => b.value(v)}.endList()
        case _ => builder.value(value)
      }
    }
  }
}
