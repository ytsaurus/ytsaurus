package org.apache.spark.sql.catalyst.catalog

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchDatabaseException, SimpleFunctionRegistry, SimpleTableFunctionRegistry, TableAlreadyExistsException, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParserInterface}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.GLOBAL_TEMP_DATABASE
import tech.ytsaurus.spyt.patch.annotations.{OriginClass, Subclass}

import java.net.URI

@Subclass
@OriginClass("org.apache.spark.sql.catalyst.catalog.SessionCatalog")
class SessionCatalogSpyt(
     externalCatalogBuilder: () => ExternalCatalog,
     globalTempViewManagerBuilder: () => GlobalTempViewManager,
     functionRegistry: FunctionRegistry,
     tableFunctionRegistry: TableFunctionRegistry,
     hadoopConf: Configuration,
     parser: ParserInterface,
     functionResourceLoader: FunctionResourceLoader,
     cacheSize: Int,
     cacheTTL: Long)
  extends SessionCatalog(
      externalCatalogBuilder,
      globalTempViewManagerBuilder,
      functionRegistry,
      tableFunctionRegistry,
      hadoopConf,
      parser,
      functionResourceLoader,
      cacheSize,
      cacheTTL) {

  // For testing only.
  def this(
      externalCatalog: ExternalCatalog,
      functionRegistry: FunctionRegistry,
      tableFunctionRegistry: TableFunctionRegistry) = {
    this(
      () => externalCatalog,
      () => new GlobalTempViewManager(SQLConf.get.getConf(GLOBAL_TEMP_DATABASE)),
      functionRegistry,
      tableFunctionRegistry,
      new Configuration(),
      new CatalystSqlParser(),
      DummyFunctionResourceLoader,
      SQLConf.get.tableRelationCacheSize,
      SQLConf.get.metadataCacheTTL)
  }

  private def callMakeQualifiedTablePath(locationUri: URI, database: String): URI = {
    val baseClass = this.getClass.getSuperclass
    val makeQualifiedTablePath = baseClass.getDeclaredMethod("makeQualifiedTablePath", classOf[URI], classOf[String])
    makeQualifiedTablePath.setAccessible(true)
    val result = makeQualifiedTablePath.invoke(this, locationUri, database).asInstanceOf[URI]
    makeQualifiedTablePath.setAccessible(false)
    result
  }

  override def createTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean,
      validateLocation: Boolean = true): Unit = {
    val isExternal = tableDefinition.tableType == CatalogTableType.EXTERNAL
    if (isExternal && tableDefinition.storage.locationUri.isEmpty) {
      throw QueryCompilationErrors.createExternalTableWithoutLocationError
    }

    val db = formatDatabaseName(tableDefinition.identifier.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(tableDefinition.identifier.table)
    val tableIdentifier = TableIdentifier(table, Some(db))

    val newTableDefinition = if (tableDefinition.storage.locationUri.isDefined
      && !tableDefinition.storage.locationUri.get.isAbsolute) {
      // make the location of the table qualified.
      val qualifiedTableLocation = callMakeQualifiedTablePath(tableDefinition.storage.locationUri.get, db)
      tableDefinition.copy(
        storage = tableDefinition.storage.copy(locationUri = Some(qualifiedTableLocation)),
        identifier = tableIdentifier)
    } else {
      tableDefinition.copy(identifier = tableIdentifier)
    }

    if (!databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }
    if (tableExists(newTableDefinition.identifier)) {
      if (!ignoreIfExists) {
        if (validateLocation) {
          throw new TableAlreadyExistsException(db = db, table = table)
        } else {
          // Table could be already created by insert operation
          logWarning("Table existence should not be ignored, but location is already validated. " +
            "So modifiable operation has inserted data already")
        }
      }
    } else if (validateLocation) {
      validateTableLocation(newTableDefinition)
    }
    externalCatalog.createTable(newTableDefinition, ignoreIfExists)
  }

  def copyStateTo(target: SessionCatalogSpyt): Unit = super.copyStateTo(target)
}
