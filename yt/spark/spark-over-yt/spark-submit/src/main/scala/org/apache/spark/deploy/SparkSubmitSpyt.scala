package org.apache.spark.deploy

import javassist.CtMethod
import javassist.bytecode.{CodeAttribute, CodeIterator, ConstPool, Mnemonic, Opcode, StackMapTable, YTsaurusBytecodeUtils}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util.{DependencyUtils, Utils}
import tech.ytsaurus.spyt.patch.MethodProcesor
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

import java.util
import scala.collection.mutable.ArrayBuffer

@Decorate
@OriginClass("org.apache.spark.deploy.SparkSubmit")
private[spark] class SparkSubmitSpyt {
  import YTsaurusConstants._
  import SparkSubmitSpyt._

  @DecoratedMethod(baseMethodProcessors = Array(classOf[ClusterManagerInitializerBytecodeModifier]))
  private[deploy] def prepareSubmitEnvironment(args: SparkSubmitArguments,
                                               conf: Option[HadoopConfiguration] = None)
  : (Seq[String], Seq[String], SparkConf, String) = {

    // Exact copy of the corresponding construct in base method
    val deployMode: Int = args.deployMode match {
      case "client" | null => CLIENT
      case "cluster" => CLUSTER
      case _ =>
        error("Deploy mode must be either client or cluster")
        -1
    }

    // Other cluster managers are processed inside __prepareSubmitEnvironment
    val clusterManager: Int = if (args.master.startsWith(YTSAURUS_MASTER)) YTSAURUS else 0
    val isYTsaurusCluster = clusterManager == YTSAURUS && deployMode == CLUSTER

    // here we use args.master.startsWith instead of STANDALONE because it should be processed inside base method
    if (args.isPython && args.master.startsWith("spark") && deployMode == CLUSTER) {
      args.mainClass = "org.apache.spark.deploy.PythonRunner"
      args.childArgs = ArrayBuffer("{{USER_JAR}}", "{{PY_FILES}}") ++ args.childArgs
      args.files = DependencyUtils.mergeFileLists(args.files, args.pyFiles)
    }

    if (clusterManager == YTSAURUS) {

      if (!Utils.classIsLoadable(YTSAURUS_CLUSTER_SUBMIT_CLASS) && !Utils.isTesting) {
        error(
          "Could not load YTSAURUS classes. " +
            "It seems that YTSAURUS libraries are not in the environment. " +
            "To add them the following steps should be performed:\n\n" +
            "1. Install ytsaurus-spyt python package via \033[1mpip install ytsaurus-spyt\033[0m\n" +
            "2. Activate SPYT configuration in environment by running " +
            "\033[1msource spyt-env\033[0m command\n")
      }

      // This property is used  to initialize ytsaurus file system which is subclass of
      // org.apache.hadoop.fs.FileSystem via spark hadoop configuration
      args.sparkProperties += "spark.hadoop.yt.proxy" -> args.master.substring("ytsaurus://".length)
    }

    // This is a potential security issue, should be fixed ASAP, see SPYT-604 for details
    val ytToken = sys.env.get("SPARK_YT_TOKEN")
    if (ytToken.isDefined) {
      args.sparkProperties += "spark.hadoop.yt.token" -> ytToken.get
    }

    var (childArgsSeq, childClasspath, sparkConf, childMainClass) = __prepareSubmitEnvironment(args, conf)

    val childArgs = new ArrayBuffer[String]()

    // This section is a copy of the corresponding section of super.prepareSubmitEnvironment
    val options = List[OptionAssigner](
      // Updated copy from SparkSubmit
      OptionAssigner(args.packages, YTSAURUS, CLUSTER, confKey = JAR_PACKAGES.key),
      OptionAssigner(args.repositories, YTSAURUS, CLUSTER, confKey = JAR_REPOSITORIES.key),
      OptionAssigner(args.ivyRepoPath, YTSAURUS, CLUSTER, confKey = JAR_IVY_REPO_PATH.key),
      OptionAssigner(args.packagesExclusions, YTSAURUS, CLUSTER, confKey = JAR_PACKAGES_EXCLUSIONS.key),

      OptionAssigner(args.numExecutors, YTSAURUS, ALL_DEPLOY_MODES, confKey = EXECUTOR_INSTANCES.key),
      OptionAssigner(args.executorCores, YTSAURUS, ALL_DEPLOY_MODES, confKey = EXECUTOR_CORES.key),
      OptionAssigner(args.executorMemory, YTSAURUS, ALL_DEPLOY_MODES, confKey = EXECUTOR_MEMORY.key),
      OptionAssigner(args.files, YTSAURUS, ALL_DEPLOY_MODES, confKey = FILES.key),
      OptionAssigner(args.archives, YTSAURUS, ALL_DEPLOY_MODES, confKey = ARCHIVES.key),
      OptionAssigner(args.jars, YTSAURUS, ALL_DEPLOY_MODES, confKey = JARS.key),
      OptionAssigner(args.driverMemory, YTSAURUS, CLUSTER, confKey = DRIVER_MEMORY.key),
      OptionAssigner(args.driverCores, YTSAURUS, CLUSTER, confKey = DRIVER_CORES.key),

      // YTsaurus only
      OptionAssigner(args.queue, YTSAURUS, ALL_DEPLOY_MODES, confKey = "spark.ytsaurus.pool"),
    )

    childArgs ++= processOptions(options, deployMode, clusterManager, sparkConf)

    if (isYTsaurusCluster) {
      childMainClass = YTSAURUS_CLUSTER_SUBMIT_CLASS
      if (args.primaryResource != SparkLauncher.NO_RESOURCE) {
        if (args.isPython) {
          childArgs ++= Array("--primary-py-file", args.primaryResource)
          childArgs ++= Array("--main-class", "org.apache.spark.deploy.PythonRunner")
        } else if (args.isR) {
          childArgs ++= Array("--primary-r-file", args.primaryResource)
          childArgs ++= Array("--main-class", "org.apache.spark.deploy.RRunner")
        }
        else {
          childArgs ++= Array("--primary-java-resource", args.primaryResource)
          childArgs ++= Array("--main-class", args.mainClass)
        }
      } else {
        childArgs ++= Array("--main-class", args.mainClass)
      }
      // TODO look at yarn or k8s cases for some python and r additional options
      // TODO maybe some YT-specific configuration
      if (args.childArgs != null) {
        appendChildArgs(args, childArgs)
      }
    }

    if (clusterManager == YTSAURUS) {
      sparkConf.set("spark.ytsaurus.primary.resource", args.primaryResource)
    }

    (childArgsSeq ++ childArgs, childClasspath, sparkConf, childMainClass)
  }

  private[deploy] def __prepareSubmitEnvironment(args: SparkSubmitArguments, conf: Option[HadoopConfiguration] = None)
  : (Seq[String], Seq[String], SparkConf, String) = ???

  @DecoratedMethod
  private def error(msg: String): Unit = {
    if (!msg.equals("Cluster deploy mode is currently not supported for python applications on standalone clusters.")) {
      __error(msg)
    }
  }

  def __error(msg: String): Unit = ???
}

object SparkSubmitSpyt {
  import YTsaurusConstants._

  // YTsaurus additions
  private val ALL_CLUSTER_MGRS: Int = {
    val cls = SparkSubmit.getClass
    val field = cls.getDeclaredField("org$apache$spark$deploy$SparkSubmit$$ALL_CLUSTER_MGRS")
    field.setAccessible(true)
    val value = field.getInt(SparkSubmit) | YTSAURUS
    field.set(SparkSubmit, value)
    field.setAccessible(false)
    value
  }

  // Exact copy from object SparkSubmit
  private val CLIENT = 1
  private val CLUSTER = 2
  private val ALL_DEPLOY_MODES = CLIENT | CLUSTER

  private[deploy] val YTSAURUS_CLUSTER_SUBMIT_CLASS =
    "org.apache.spark.deploy.ytsaurus.YTsaurusClusterApplication"

  private[deploy] def processOptions(options: List[OptionAssigner],
                                     deployMode: Int,
                                     clusterManager: Int,
                                     sparkConf: SparkConf): Seq[String] = {
    val childArgs = new ArrayBuffer[String]()
    for (opt <- options) {
      if (opt.value != null &&
        (deployMode & opt.deployMode) != 0 &&
        (clusterManager & opt.clusterManager) != 0) {
        if (opt.clOption != null) { childArgs += (opt.clOption, opt.value) }
        if (opt.confKey != null) {
          if (opt.mergeFn.isDefined && sparkConf.contains(opt.confKey)) {
            sparkConf.set(opt.confKey, opt.mergeFn.get.apply(sparkConf.get(opt.confKey), opt.value))
          } else {
            sparkConf.set(opt.confKey, opt.value)
          }
        }
      }
    }
    childArgs
  }

  private[deploy] def appendChildArgs(args: SparkSubmitArguments, childArgs: ArrayBuffer[String]): Unit = {
    args.childArgs.foreach { arg =>
      childArgs += ("--arg", arg)
    }
  }
}

object YTsaurusConstants {
  val YTSAURUS = 32
  val YTSAURUS_MASTER = "ytsaurus"
}

class ClusterManagerInitializerBytecodeModifier extends MethodProcesor {
  import YTsaurusBytecodeUtils._
  import YTsaurusConstants._

  override def process(method: CtMethod): Unit = {
    val methodInfo = method.getMethodInfo
    val cp = methodInfo.getConstPool
    val ca = methodInfo.getCodeAttribute

    updateErrorMessage(cp)
    addYTsaurusClause(cp, ca)
  }

  private def addYTsaurusClause(cp: ConstPool, ca: CodeAttribute): Unit = {
    val localConstId = findStringConstant(cp, _.equals("local"))

    val ci = ca.iterator()

    // Looking for start of the "local" cluster manager clause
    var found: Boolean = false
    var samplePos = 0
    while (ci.hasNext && !found) {
      val index = ci.next()
      val op = ci.byteAt(index);
      if (op == Opcode.ALOAD && ci.byteAt(ci.lookAhead()) == Opcode.LDC_W) {
        val ldcwOpIndex = ci.next()
        val ldcwArgument = ci.u16bitAt(ldcwOpIndex + 1)
        if (ldcwArgument == localConstId) {
          found = true
          samplePos = index
        }
      }
    }

    // Looking for the end of the "local" cluster manager clause
    found = false
    var insertPos = 0
    while (ci.hasNext && !found) {
      val index = ci.next()
      val op = ci.byteAt(index)
      if (op == Opcode.GOTO && ci.u16bitAt(index + 1) == 3) { // found goto 3
        found = true
        insertPos = ci.lookAhead()
      }
    }

    val sampleLength = insertPos - samplePos
    val sample = new Array[Byte](sampleLength)

    System.arraycopy(ca.getCode, samplePos, sample, 0, sampleLength)

    val ytsaurusConstId = cp.addStringInfo(YTSAURUS_MASTER)
    val ytsaurusCodeId = cp.addIntegerInfo(YTSAURUS)

    val si = new CodeAttribute(cp, ca.getMaxStack, ca.getMaxLocals, sample, ca.getExceptionTable).iterator

    var skipInvokeVirtual = false
    val sameFramePositions = new java.util.ArrayList[Integer]()
    while (si.hasNext) {
      val index = si.next
      val op = si.byteAt(index)

      if (op == Opcode.LDC_W) {
        si.write16bit(ytsaurusConstId, index + 1)
      } else if (op == Opcode.GETSTATIC) {
        skipInvokeVirtual = true
        si.writeByte(Opcode.LDC_W, index)
        si.write16bit(ytsaurusCodeId, index + 1)
      } else if (op == Opcode.INVOKEVIRTUAL && skipInvokeVirtual) {
        // fill second invokevirtual with nop because we use constant instead of accessing object fields
        for (pos <- index until si.lookAhead) {
          si.writeByte(Opcode.NOP, pos)
        }
      } else if (op == Opcode.IFEQ || op == Opcode.GOTO) {
        val offset = si.u16bitAt(index + 1)
        if (index + offset <= sampleLength) {
          sameFramePositions.add(insertPos + index + offset)
        }
      }
    }

    ci.insertAt(insertPos, sample)

    val originTable = ca.getAttribute(StackMapTable.tag).asInstanceOf[StackMapTable]
    val modifiedTable = addStackMapTableFrames(originTable, sameFramePositions, insertPos, sampleLength)
    ca.setAttribute(modifiedTable)
  }

  private def updateErrorMessage(cp: ConstPool): Unit = {
    val invalidClusterManagerMsgId = findStringConstant(cp, _.startsWith("Master must either"))
    val invalidClusterManagerMsg = cp.getStringInfo(invalidClusterManagerMsgId);
    updateUtf8Constant(
      cp, getUtf8ConstantId(cp, invalidClusterManagerMsgId),
      invalidClusterManagerMsg.replace("k8s,", "k8s, ytsaurus")
    )
  }
}