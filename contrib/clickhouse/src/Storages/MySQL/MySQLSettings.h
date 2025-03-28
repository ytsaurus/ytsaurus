#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>


namespace DBPoco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class ASTStorage;
class ASTSetQuery;

#define LIST_OF_MYSQL_SETTINGS(M, ALIAS) \
    M(UInt64, connection_pool_size, 16, "Size of connection pool (if all connections are in use, the query will wait until some connection will be freed).", 0) \
    M(UInt64, connection_max_tries, 3, "Number of retries for pool with failover", 0) \
    M(UInt64, connection_wait_timeout, 5, "Timeout (in seconds) for waiting for free connection (in case of there is already connection_pool_size active connections), 0 - do not wait.", 0) \
    M(Bool, connection_auto_close, true, "Auto-close connection after query execution, i.e. disable connection reuse.", 0) \
    M(UInt64, connect_timeout, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, "Connect timeout (in seconds)", 0) \
    M(UInt64, read_write_timeout, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, "Read/write timeout (in seconds)", 0) \
    M(MySQLDataTypesSupport, mysql_datatypes_support_level, MySQLDataTypesSupportList{}, "Which MySQL types should be converted to corresponding ClickHouse types (rather than being represented as String). Can be empty or any combination of 'decimal' or 'datetime64'. When empty MySQL's DECIMAL and DATETIME/TIMESTAMP with non-zero precision are seen as String on ClickHouse's side.", 0) \

DECLARE_SETTINGS_TRAITS(MySQLSettingsTraits, LIST_OF_MYSQL_SETTINGS)


using MySQLBaseSettings = BaseSettings<MySQLSettingsTraits>;

/** Settings for the MySQL family of engines.
  */
struct MySQLSettings : public MySQLBaseSettings
{
    void loadFromQuery(ASTStorage & storage_def);
    void loadFromQuery(const ASTSetQuery & settings_def);
    void loadFromQueryContext(ContextPtr context, ASTStorage & storage_def);
};


}
