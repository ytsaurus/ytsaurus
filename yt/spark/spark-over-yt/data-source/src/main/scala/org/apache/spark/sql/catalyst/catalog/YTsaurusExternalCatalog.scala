package org.apache.spark.sql.catalyst.catalog

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.serializers.{SchemaConverter, SchemaConverterConfig}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider
import tech.ytsaurus.spyt.wrapper.table.BaseYtTableSettings

import java.net.URI
import java.util.UUID

class YTsaurusExternalCatalog(conf: SparkConf, hadoopConf: Configuration)
  extends InMemoryCatalog(conf, hadoopConf) with Logging {

  private val id: String = s"YTsaurusExternalCatalog-${UUID.randomUUID()}"
  private val yt = YtClientProvider.ytClient(ytClientConfiguration(hadoopConf), id)

  private val YTSAURUS_DB = "yt"

  private lazy val ytDatabase = CatalogDatabase(YTSAURUS_DB, "", new URI(""), Map.empty)

  private def isYtDatabase(db: String): Boolean = db == YTSAURUS_DB
  private def isYtDatabase(db: Option[String]): Boolean = db.exists(isYtDatabase)

  private def checkUnsupportedDatabase(db: String): Unit = {
    if (isYtDatabase(db)) {
      throw new IllegalStateException("Operation is not supported for YTsaurus tables")
    }
  }

  private def checkUnsupportedDatabase(db: Option[String]): Unit = db.foreach(checkUnsupportedDatabase)

  override def databaseExists(db: String): Boolean = isYtDatabase(db) || super.databaseExists(db)

  override def getDatabase(db: String): CatalogDatabase = {
    if (isYtDatabase(db)) {
      ytDatabase
    } else {
      super.getDatabase(db)
    }
  }

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = synchronized {
    if (isYtDatabase(tableDefinition.identifier.database)) {
      val path = YtWrapper.formatPath(tableDefinition.identifier.table)
      if (!YtWrapper.exists(path)(yt)) {
        val tableSchema = SchemaConverter.tableSchema(tableDefinition.schema)
        val settings = new BaseYtTableSettings(tableSchema)
        YtWrapper.createTable(path, settings)(yt)
      } else {
        logWarning("Table is created twice. Ignoring new query")
      }
    } else {
      super.createTable(tableDefinition, ignoreIfExists)
    }
  }

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = synchronized {
    if (isYtDatabase(db)) {
      val path = YtWrapper.formatPath(table)
      if (ignoreIfNotExists) {
        YtWrapper.removeIfExists(path)(yt)
      } else {
        YtWrapper.remove(path)(yt)
      }
    } else {
      super.dropTable(db, table, ignoreIfNotExists, purge)
    }
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    checkUnsupportedDatabase(db)
    super.renameTable(db, oldName, newName)
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    checkUnsupportedDatabase(tableDefinition.identifier.database)
    super.alterTable(tableDefinition)
  }

  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit = {
    checkUnsupportedDatabase(db)
    super.alterTableDataSchema(db, table, newDataSchema)
  }

  override def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit = {
    checkUnsupportedDatabase(db)
    super.alterTableStats(db, table, stats)
  }

  private def getYtTableOptional(table: String): Option[CatalogTable] = {
    val path = YtWrapper.formatPath(table)
    if (YtWrapper.exists(path)(yt)) {
      val ident = TableIdentifier(table, Some(YTSAURUS_DB))
      val config = SchemaConverterConfig(conf)
      val schemaTree = YtWrapper.attribute(path, "schema")(yt)
      val schema = SchemaConverter.sparkSchema(schemaTree, parsingTypeV3 = config.parsingTypeV3)
      val storage = CatalogStorageFormat(
        locationUri = Some(new URI(table)),
        inputFormat = None, outputFormat = None, serde = None, compressed = false, properties = Map.empty
      )
      Some(CatalogTable(ident, CatalogTableType.MANAGED, storage, schema, provider = Some("yt")))
    } else {
      None
    }
  }

  override def getTable(db: String, table: String): CatalogTable = synchronized {
    if (isYtDatabase(db)) {
      getYtTableOptional(table).getOrElse {
        throw new NoSuchTableException(db = db, table = table)
      }
    } else {
      super.getTable(db, table)
    }
  }

  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = synchronized {
    if (isYtDatabase(db)) {
      tables.flatMap(getYtTableOptional)
    } else {
      super.getTablesByName(db, tables)
    }
  }

  override def tableExists(db: String, table: String): Boolean = {
    if (isYtDatabase(db)) {
      val path = YtWrapper.formatPath(table)
      YtWrapper.exists(path)(yt)
    } else {
      super.tableExists(db, table)
    }
  }

  override def listTables(db: String): Seq[String] = {
    checkUnsupportedDatabase(db)
    super.listTables(db)
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    checkUnsupportedDatabase(db)
    super.listTables(db, pattern)
  }

  override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean,
                         isSrcLocal: Boolean): Unit = {
    checkUnsupportedDatabase(db)
    super.loadTable(db, table, loadPath, isOverwrite, isSrcLocal)
  }
}
