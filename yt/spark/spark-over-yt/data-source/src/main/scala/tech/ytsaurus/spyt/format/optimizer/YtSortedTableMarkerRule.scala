package tech.ytsaurus.spyt.format.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.v2.YtScan.ScanDescription
import org.apache.spark.sql.v2.{YtFilePartition, YtScan}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.format.optimizer.YtSortedTableMarkerRule.{findAttributes, getVars, getYtScan, parseAndClauses, prepareScanDesc, replaceYtScan}

import scala.annotation.tailrec

class YtSortedTableMarkerRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    import tech.ytsaurus.spyt.fs.conf._
    if (spark.ytConf(SparkYtConfiguration.Read.PlanOptimizationEnabled)) {
      logInfo("Plan optimization try")
      transformPlan(plan)
    } else {
      logInfo("Plan optimization is disabled")
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
      } yield {
        val clauses = parseAndClauses(condition)
        val attrs = findAttributes(left.output, right.output, clauses)
        if (attrs.isEmpty) {
          join
        } else {
          patchJoin(join, attrs)
        }
      }
      res.getOrElse(join)
  }

  private def patchJoin(join: Join, attrs: Seq[(AttributeReference, AttributeReference)]): Join = {
    val (attributesL, attributesR) = attrs.unzip
    val (leftNewScanDescO, rightNewScanDescO) =
      YtScan.trySyncKeyPartitioning(prepareScanDesc(join.left, attributesL), prepareScanDesc(join.right, attributesR))
    logInfo(
      s"Join optimization is tested. " +
        s"Left: ${leftNewScanDescO.isDefined}, right: ${rightNewScanDescO.isDefined}")
    (leftNewScanDescO, rightNewScanDescO) match {
      case (Some((leftNewScan, leftVars)), Some((rightNewScan, rightVars))) =>
        join.copy(
          left = LogicalHashedMarker(leftVars, replaceYtScan(join.left, leftNewScan)),
          right = LogicalHashedMarker(rightVars, replaceYtScan(join.right, rightNewScan))
        )
      case (Some((leftNewScan, leftVars)), None) =>
        val leftPivots = YtFilePartition.getPivotFromHintFiles(leftVars, leftNewScan.keyPartitionsHint.get)
        join.copy(
          left = LogicalHashedMarker(leftVars, replaceYtScan(join.left, leftNewScan)),
          right = LogicalDependentHashMarker(attributesR, leftPivots, join.right)
        )
      case (None, Some((rightNewScan, rightVars))) =>
        val rightPivots = YtFilePartition.getPivotFromHintFiles(rightVars, rightNewScan.keyPartitionsHint.get)
        join.copy(
          left = LogicalDependentHashMarker(attributesL, rightPivots, join.left),
          right = LogicalHashedMarker(rightVars, replaceYtScan(join.right, rightNewScan))
        )
      case (None, None) =>
        join
    }
  }
}

object YtSortedTableMarkerRule {
  def prepareScanDesc(node: LogicalPlan, expressions: Seq[Expression]): Option[ScanDescription] = {
    getYtScan(node).zip(getVars(expressions)).headOption
  }

  private def findAttributes(left: Seq[Attribute], right: Seq[Attribute],
                             clauses: Seq[(AttributeReference, AttributeReference)]): Seq[(AttributeReference, AttributeReference)] = {
    clauses.filter { case (cL, cR) =>
      left.exists { case aL: AttributeReference => cL.sameRef(aL) } &&
        right.exists { case aR: AttributeReference => cR.sameRef(aR) }
    }
  }

  private def parseAndClauses(condition: Expression): Seq[(AttributeReference, AttributeReference)] = {
    condition match {
      case EqualTo(aL: AttributeReference, aR: AttributeReference) => Seq((aL, aR))
      case And(left, right) => parseAndClauses(left) ++ parseAndClauses(right)
      case _ => Seq()
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
      case Project(_, child) => getYtScan(child)
      case Filter(_, child) => getYtScan(child)
      case DataSourceV2ScanRelation(_, scan: YtScan, _) => Some(scan)
      case _ => None
    }
  }

  private def replaceYtScan(node: LogicalPlan, newYtScan: YtScan): LogicalPlan = {
    node match {
      case p@Project(_, child) => p.copy(child = replaceYtScan(child, newYtScan))
      case f@Filter(_, child) => f.copy(child = replaceYtScan(child, newYtScan))
      case r@DataSourceV2ScanRelation(_, _: YtScan, _) => r.copy(scan = newYtScan)
      case _ => throw new IllegalArgumentException("Couldn't replace yt scan, optimization broke execution plan")
    }
  }
}
