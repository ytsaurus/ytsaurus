package tech.ytsaurus.spark.launcher

import com.codahale.metrics.MetricRegistry
import org.slf4j.{Logger, LoggerFactory}
import tech.ytsaurus.spark.launcher.AdditionalMetricsSender.MetricsConfig.MetricsConfig
import tech.ytsaurus.spark.metrics.{ReporterConfig, SolomonReporter, SolomonConfig => SC}

import scala.util.Try

trait AdditionalMetricsSender {
  def start(): Unit
  def stop(): Unit
}

object AdditionalMetricsSender {
  val log: Logger = LoggerFactory.getLogger(AdditionalMetricsSender.getClass)

  def apply(systemProps: Map[String, String], instance: String, registry: MetricRegistry): AdditionalMetricsSender = {
    val conf = new MetricsConfig(systemProps)
    conf.initialize()
    val instProps = conf.getInstance(instance)
    val propsOpt = conf.subProperties(instProps, "^sink\\.(.+)\\.(.+)".r).get("solomon")

    val reporter = for {
      props <- Try(propsOpt.get)
      solomonConfig <- Try(SC.read(props))
      reporterConfig <- Try(ReporterConfig.read(props))
      reporter <- SolomonReporter.tryCreateSolomonReporter(registry, solomonConfig, reporterConfig)
    } yield reporter

    reporter.failed.foreach { ex =>
      log.error(s"Failed to create solomon reporter: ${ex.getMessage}", ex)
    }

    new AdditionalMetricsSender {
      override def start(): Unit = reporter.foreach(_.start())
      override def stop(): Unit = reporter.foreach(_.stop())
    }
  }

  object MetricsConfig {
    /* taken from org.apache.spark.MetricsConfig, spark deps removed */
    import java.io.{FileInputStream, InputStream}
    import java.util.Properties
    import scala.collection.JavaConverters._
    import scala.collection.mutable
    import scala.util.matching.Regex

    class MetricsConfig(systemProps: Map[String, String]) { // no internal.logging, spark conf replaced with props

      private val DEFAULT_PREFIX = "*"
      private val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
      private val DEFAULT_METRICS_CONF_FILENAME = "metrics.properties"

      val properties = new Properties()
      private var perInstanceSubProperties: mutable.HashMap[String, Properties] = null

      private def setDefaultProperties(prop: Properties): Unit = {
        prop.setProperty("*.sink.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet")
        prop.setProperty("*.sink.servlet.path", "/metrics/json")
        prop.setProperty("master.sink.servlet.path", "/metrics/master/json")
        prop.setProperty("applications.sink.servlet.path", "/metrics/applications/json")
      }

      /**
       * Load properties from various places, based on precedence
       * If the same property is set again latter on in the method, it overwrites the previous value
       */
      def initialize(): Unit = {
        // Add default properties in case there's no properties file
        setDefaultProperties(properties)
        val spytHome = sys.env.get("SPYT_HOME").map(p => p.replaceAll("\\$HOME", sys.env.getOrElse("HOME", "")))
        // constant from config package replaced with plain string
        loadPropertiesFromFile(
          systemProps.get("spark.metrics.conf")
            .orElse(spytHome.map(p => s"$p/conf/metrics.properties"))
        )

        // Also look for the properties in provided Spark configuration
        val prefix = "spark.metrics.conf."
        systemProps.foreach {
          case (k, v) if k.startsWith(prefix) =>
            properties.setProperty(k.substring(prefix.length()), v)
          case _ =>
        }

        // Now, let's populate a list of sub-properties per instance, instance being the prefix that
        // appears before the first dot in the property name.
        // Add to the sub-properties per instance, the default properties (those with prefix "*"), if
        // they don't have that exact same sub-property already defined.
        //
        // For example, if properties has ("*.class"->"default_class", "*.path"->"default_path,
        // "driver.path"->"driver_path"), for driver specific sub-properties, we'd like the output to be
        // ("driver"->Map("path"->"driver_path", "class"->"default_class")
        // Note how class got added to based on the default property, but path remained the same
        // since "driver.path" already existed and took precedence over "*.path"
        //
        perInstanceSubProperties = subProperties(properties, INSTANCE_REGEX)
        if (perInstanceSubProperties.contains(DEFAULT_PREFIX)) {
          val defaultSubProperties = perInstanceSubProperties(DEFAULT_PREFIX).asScala
          for ((instance, prop) <- perInstanceSubProperties if (instance != DEFAULT_PREFIX);
               (k, v) <- defaultSubProperties if (prop.get(k) == null)) {
            prop.put(k, v)
          }
        }
      }

      /**
       * Take a simple set of properties and a regex that the instance names (part before the first dot)
       * have to conform to. And, return a map of the first order prefix (before the first dot) to the
       * sub-properties under that prefix.
       *
       * For example, if the properties sent were Properties("*.sink.servlet.class"->"class1",
       * "*.sink.servlet.path"->"path1"), the returned map would be
       * Map("*" -> Properties("sink.servlet.class" -> "class1", "sink.servlet.path" -> "path1"))
       * Note in the subProperties (value of the returned Map), only the suffixes are used as property
       * keys.
       * If, in the passed properties, there is only one property with a given prefix, it is still
       * "unflattened". For example, if the input was Properties("*.sink.servlet.class" -> "class1"
       * the returned Map would contain one key-value pair
       * Map("*" -> Properties("sink.servlet.class" -> "class1"))
       * Any passed in properties, not complying with the regex are ignored.
       *
       * @param prop  the flat list of properties to "unflatten" based on prefixes
       * @param regex the regex that the prefix has to comply with
       * @return an unflatted map, mapping prefix with sub-properties under that prefix
       */
      def subProperties(prop: Properties, regex: Regex): mutable.HashMap[String, Properties] = {
        val subProperties = new mutable.HashMap[String, Properties]
        prop.asScala.foreach { kv =>
          if (regex.findPrefixOf(kv._1).isDefined) {
            val regex(prefix, suffix) = kv._1
            subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2.toString)
          }
        }
        subProperties
      }

      def getInstance(inst: String): Properties = {
        perInstanceSubProperties.get(inst) match {
          case Some(s) => s
          case None => perInstanceSubProperties.getOrElse(DEFAULT_PREFIX, new Properties)
        }
      }

      /**
       * Loads configuration from a config file. If no config file is provided, try to get file
       * in class path.
       */
      private[this] def loadPropertiesFromFile(path: Option[String]): Unit = {
        var is: InputStream = null
        try {
          is = path match {
            case Some(f) => new FileInputStream(f)
            // changed to this class classloader
            case None => getClass.getClassLoader.getResourceAsStream(DEFAULT_METRICS_CONF_FILENAME)
          }

          if (is != null) {
            properties.load(is)
          }
        } catch {
          case e: Exception =>
            val file = path.getOrElse(DEFAULT_METRICS_CONF_FILENAME)
            log.error(s"Error loading configuration file $file", e)
        } finally {
          if (is != null) {
            is.close()
          }
        }
      }

    }
  }
}
