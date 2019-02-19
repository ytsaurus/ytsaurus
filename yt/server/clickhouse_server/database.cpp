#include "database.h"

#include "storage_table.h"
#include "storage_stub.h"
#include "type_helpers.h"

#include <yt/server/clickhouse_server/query_context.h>
#include <yt/server/clickhouse_server/table.h>

#include <Common/Exception.h>
#include <Common/LRUCache.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace DB {

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

}   // namespace DB

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TDatabase
    : public IDatabase
{
private:
    const IExecutionClusterPtr Cluster;

public:
    TDatabase(IExecutionClusterPtr cluster)
        : Cluster(std::move(cluster))
    {}

    std::string getEngineName() const override;

    void loadTables(
        Context& context,
        ThreadPool* thread_pool,
        bool hasForceRestoreDataFlag) override;

    bool isTableExist(
        const Context& context,
        const std::string& name) const override;

    StoragePtr tryGetTable(
        const Context& context,
        const std::string& name) const override;

    DatabaseIteratorPtr getIterator(const Context& context) override;

    bool empty(const Context& context) const override;

    void createTable(
        const Context& context,
        const std::string& name,
        const StoragePtr& table,
        const ASTPtr& query) override;

    void removeTable(
        const Context& context,
        const std::string& name) override;

    void attachTable(
        const std::string& name,
        const StoragePtr& table) override;

    StoragePtr detachTable(const std::string& name) override;

    void renameTable(
        const Context& context,
        const std::string& name,
        IDatabase& newDatabase,
        const std::string& newName) override;

    void alterTable(
        const Context& context,
        const std::string& name,
        const ColumnsDescription & columns,
        const ASTModifier& engineModifier) override;

    time_t getTableMetadataModificationTime(
        const Context& context,
        const std::string& name) override;

    ASTPtr getCreateTableQuery(
        const Context& context,
        const std::string& name) const override;

    ASTPtr tryGetCreateTableQuery(
        const Context& context,
        const std::string& name) const override;

    ASTPtr getCreateDatabaseQuery(
        const Context &) const override;

    std::string getDatabaseName() const override;

    void shutdown() override;

    void drop() override;

private:
    StoragePtr GetTable(
        const Context& context,
        const std::string& name) const;
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr TDatabase::GetTable(
    const Context& context,
    const std::string& name) const
{
    auto* queryContext = GetQueryContext(context);
    auto table = queryContext->GetTable(TString(name));
    if (!table) {
        // table not found
        return nullptr;
    }

    return CreateStorageTable(std::move(table), Cluster);
}

////////////////////////////////////////////////////////////////////////////////

std::string TDatabase::getEngineName() const
{
    return "YT";
}

void TDatabase::loadTables(
    Context& /* context */,
    ThreadPool* /* thread_pool */,
    bool /* hasForceRestoreDataFlag */)
{
    // nothing to do
}

bool TDatabase::isTableExist(
    const Context& context,
    const std::string& name) const
{
    return GetTable(context, name) != nullptr;
}

StoragePtr TDatabase::tryGetTable(
    const Context& context,
    const std::string& name) const
{
    return GetTable(context, name);
}

DatabaseIteratorPtr TDatabase::getIterator(const Context& /* context */)
{
    class TDummyIterator
        : public IDatabaseIterator
    {
    public:
        bool isValid() const override
        {
            return false;
        }

        virtual void next() override
        {
            Y_UNREACHABLE();
        }

        virtual const String& name() const override
        {
            Y_UNREACHABLE();
        }

        virtual StoragePtr& table() const override
        {
            Y_UNREACHABLE();
        }
    };

    return std::make_unique<TDummyIterator>();
}

bool TDatabase::empty(const Context& /* context */) const
{
    // it is too expensive to check
    return false;
}

void TDatabase::createTable(
    const Context& /* context */,
    const std::string& /* name */,
    const StoragePtr& /* table */,
    const ASTPtr& /* query */)
{
    throw Exception(
        "TDatabase: createTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

void TDatabase::removeTable(
    const Context& /* context */,
    const std::string& /* name */)
{
    throw Exception(
        "TDatabase: removeTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

void TDatabase::attachTable(
    const std::string& /* name */,
    const StoragePtr& /* table */)
{
    throw Exception(
        "TDatabase: attachTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

StoragePtr TDatabase::detachTable(const std::string& /* name */)
{
    throw Exception(
        "TDatabase: detachTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

void TDatabase::renameTable(
    const Context& /* context */,
    const std::string& /* name */,
    IDatabase& /* newDatabase */,
    const std::string& /* newName */)
{
    throw Exception(
        "TDatabase: renameTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

void TDatabase::alterTable(
    const Context& /* context */,
    const std::string& /* name */,
    const ColumnsDescription & /* columns */,
    const ASTModifier& /* engineModifier */)
{
    throw Exception(
        "TDatabase: alterTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

time_t TDatabase::getTableMetadataModificationTime(
    const Context& /* context */,
    const std::string& /* name */)
{
    // have no idea what is that
    return 0;
}

ASTPtr TDatabase::getCreateTableQuery(
    const Context& /* context */,
    const std::string& /* name */) const
{
    throw Exception(
        "TDatabase: getCreateTableQuery() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

ASTPtr TDatabase::tryGetCreateTableQuery(
    const Context& /* context */,
    const std::string& /* name */) const
{
    throw Exception(
        "TDatabase: tryGetCreateTableQuery() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

ASTPtr TDatabase::getCreateDatabaseQuery(
    const Context& /* context */) const
{
     throw Exception(
        "TDatabase: getCreateDatabaseQuery() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

std::string TDatabase::getDatabaseName() const
{
    return {};
};

void TDatabase::shutdown()
{
    // nothing to do
}

void TDatabase::drop()
{
    // nothing to do
}

////////////////////////////////////////////////////////////////////////////////

DatabasePtr CreateDatabase(IExecutionClusterPtr cluster)
{
    return std::make_shared<TDatabase>(std::move(cluster));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
