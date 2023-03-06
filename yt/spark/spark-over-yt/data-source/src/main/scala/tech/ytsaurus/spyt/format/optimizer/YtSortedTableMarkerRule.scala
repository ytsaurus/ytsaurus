package tech.ytsaurus.spyt.format.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.v2.YtScan.ScanDescription
import org.apache.spark.sql.v2.{YtFilePartition, YtScan}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.format.optimizer.YtSortedTableMarkerRule.{getVars, getYtScan, parseAndClauses, replaceYtScan}

import scala.annotation.tailrec

class YtSortedTableMarkerRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    import tech.ytsaurus.spyt.fs.conf._
    if (spark.ytConf(SparkYtConfiguration.Read.PlanOptimizationEnabled)) {
      logInfo("Plan optimization try")
      transformPlan(plan)
    } else {
      plan
    }
  }

  private def transformPlan(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case agg@Aggregate(_, _, inner) =>
      val res = for {
        vars <- getVars(agg.groupingExpressions)
        scan <- getYtScan(inner)
        newScan <- scan.tryKeyPartitioning(Some(vars))
      } yield {
        agg.copy(child = LogicalSortedMarker(vars, replaceYtScan(inner, newScan)))
      }
      res.getOrElse(agg)
    case join@Join(left, right, Inner, _, _) =>
      val res = for {
        condition <- join.condition
        expressions <- parseAndClauses(condition)
      } yield {
        def prepareScanDesc(node: LogicalPlan, expressions: Seq[Expression]): Option[ScanDescription] = {
          getYtScan(node).zip(getVars(expressions)).headOption
        }

        val (expressionsL, expressionsR) = expressions.unzip
        val (leftNewScanDescO, rightNewScanDescO) =
          YtScan.trySyncKeyPartitioning(prepareScanDesc(left, expressionsL), prepareScanDesc(right, expressionsR))
        logInfo(
          s"Join optimization is tested. " +
            s"Left: ${leftNewScanDescO.isDefined}, right: ${rightNewScanDescO.isDefined}")
        (leftNewScanDescO, rightNewScanDescO) match {
          case (Some((leftNewScan, leftVars)), Some((rightNewScan, rightVars))) =>
            join.copy(
              left = LogicalHashedMarker(leftVars, replaceYtScan(left, leftNewScan)),
              right = LogicalHashedMarker(rightVars, replaceYtScan(right, rightNewScan))
            )
          case (Some((leftNewScan, leftVars)), None) =>
            val leftPivots = YtFilePartition.getPivotFromHintFiles(leftVars, leftNewScan.keyPartitionsHint.get)
            join.copy(
              left = LogicalHashedMarker(leftVars, replaceYtScan(left, leftNewScan)),
              right = LogicalDependentHashMarker(expressionsR, leftPivots, right)
            )
          case (None, Some((rightNewScan, rightVars))) =>
            val rightPivots = YtFilePartition.getPivotFromHintFiles(rightVars, rightNewScan.keyPartitionsHint.get)
            join.copy(
              left = LogicalDependentHashMarker(expressionsL, rightPivots, left),
              right = LogicalHashedMarker(rightVars, replaceYtScan(right, rightNewScan))
            )
          case (None, None) =>
            join
        }
      }
      res.getOrElse(join)
  }
}

object YtSortedTableMarkerRule {
  private def mergeHalves[T](leftClauses: Option[Seq[T]], rightClauses: Option[Seq[T]]): Option[Seq[T]] = {
    for {
      parsedLeft <- leftClauses
      parsedRight <- rightClauses
    } yield {
      parsedLeft ++ parsedRight
    }
  }

  private def parseAndClauses(condition: Expression): Option[Seq[(Expression, Expression)]] = {
    condition match {
      case EqualTo(aL: Expression, aR: Expression) => Some(Seq((aL, aR)))
      case And(left, right) => mergeHalves(parseAndClauses(left), parseAndClauses(right))
      case _ => None
    }
  }

  private def getVars(expressions: Seq[Expression]): Option[Seq[String]] = {
    val attrs = expressions.map {
      case a: AttributeReference => Some(a.name)
      case _ => None
    }
    if (attrs.forall(_.isDefined)) {
      Some(attrs.map(_.get))
    } else {
      None
    }
  }

  @tailrec
  private def getYtScan(node: LogicalPlan): Option[YtScan] = {
    node match {
      case Filter(_, child) => getYtScan(child)
      case DataSourceV2ScanRelation(_, scan: YtScan, _) => Some(scan)
      case _ => None
    }
  }

  private def replaceYtScan(node: LogicalPlan, newYtScan: YtScan): LogicalPlan = {
    node match {
      case f@Filter(_, child) => f.copy(child = replaceYtScan(child, newYtScan))
      case r@DataSourceV2ScanRelation(_, _: YtScan, _) => r.copy(scan = newYtScan)
      case _ => throw new IllegalArgumentException("Couldn't replace yt scan, optimization broke execution plan")
    }
  }
}
