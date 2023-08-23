package tech.ytsaurus.spyt.format

import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedRowDeserializer}
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.ColumnValueType
import tech.ytsaurus.spyt.wrapper.YtWrapper

object YtDataCheck extends Matchers {

  def yPathShouldContainExpectedData[T <: Product : Ordering, U: Ordering]
  (yPath: YPath, expectedData: Seq[T])(rowKeyExtractor: UnversionedRow => U)(implicit yt: CompoundClient): Unit = {
    val resultRows = YtWrapper.readTable(
      yPath,
      new UnversionedRowDeserializer(),
      reportBytesRead = (_ => ())
    ).toSeq

    resultRows.size shouldEqual expectedData.size

    val resultRowsSorted = resultRows.sortBy(rowKeyExtractor)

    val expectedDataSorted = expectedData.sorted

    (resultRowsSorted zip expectedDataSorted).foreach { case (resultRow, expectedRow) =>
      val resultRowValues = resultRow.getValues
      resultRowValues.size() shouldEqual expectedRow.productArity

      (0 until expectedRow.productArity).foreach { pos =>
        val cell = resultRowValues.get(pos)
        val cellValue = if (cell.getType == ColumnValueType.STRING) {
          cell.stringValue()
        } else {
          cell.getValue
        }
        cellValue shouldEqual expectedRow.productElement(pos)
      }
    }
  }
}
