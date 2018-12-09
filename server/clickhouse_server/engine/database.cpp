#include "database.h"

#include "auth_token.h"
#include "storage_table.h"
#include "storage_stub.h"
#include "type_helpers.h"

#include <yt/server/clickhouse_server/native/storage.h>
#include <yt/server/clickhouse_server/native/table_schema.h>

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

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TUserTables {
    Tables UserTables;
    std::mutex Mutex;
};

using TUserTablesCache = LRUCache<std::string, TUserTables>;

const size_t CacheSize = 100;
const size_t CacheExpirationDelay = 300;  // seconds

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TDatabase
    : public IDatabase
{
private:
    const NNative::IStoragePtr Storage;
    const IExecutionClusterPtr Cluster;

    mutable TUserTablesCache UserTablesCache;

public:
    TDatabase(NNative::IStoragePtr storage, IExecutionClusterPtr cluster)
        : Storage(std::move(storage))
        , Cluster(std::move(cluster))
        , UserTablesCache(CacheSize, TUserTablesCache::Delay(CacheExpirationDelay))
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
    TUserTablesCache::MappedPtr GetUserTables(const Context& context) const;

    StoragePtr GetTable(
        const Context& context,
        const TUserTablesCache::MappedPtr& userTables,
        const std::string& name) const;
};

////////////////////////////////////////////////////////////////////////////////

class TDatabaseIterator
    : public IDatabaseIterator
{
private:
    const NNative::TTableList Tables;

    NNative::TTableList::const_iterator Current;

    mutable std::string CurrentName;
    mutable StoragePtr CurrentTable;

public:
    TDatabaseIterator(NNative::TTableList tables)
        : Tables(std::move(tables))
        , Current(Tables.begin())
    {}

    bool isValid() const override
    {
        return Current != Tables.end();
    }

    const std::string& name() const override
    {
        if (CurrentName.empty()) {
            CurrentName = ToStdString((*Current)->Name);
        }
        return CurrentName;
    }

    StoragePtr& table() const override
    {
        if (!CurrentTable) {
            CurrentTable = CreateStorageStub(*Current);
        }
        return CurrentTable;
    }

    void next() override
    {
        ++Current;
        CurrentName.clear();
        CurrentTable.reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

TUserTablesCache::MappedPtr TDatabase::GetUserTables(const Context& context) const
{
    const auto& clientInfo = context.getClientInfo();

    TUserTablesCache::MappedPtr userTables;
    bool created;

    std::tie(userTables, created) = UserTablesCache.getOrSet(
        clientInfo.current_user,
        [] { return std::make_shared<TUserTables>(); });

    return userTables;
}

StoragePtr TDatabase::GetTable(
    const Context& context,
    const TUserTablesCache::MappedPtr& userTables,
    const std::string& name) const
{
    {
        // lookup for cached tables first
        std::lock_guard<std::mutex> lock(userTables->Mutex);

        auto it = userTables->UserTables.find(name);
        if (it != userTables->UserTables.end()) {
            return it->second;
        }
    }

    auto token = CreateAuthToken(*Storage, context);

    auto table = Storage->GetTable(*token, ToString(name));
    if (!table) {
        // table not found
        return nullptr;
    }

    auto storage = CreateStorageTable(Storage, std::move(table), Cluster);

    {
        // store table in the cache
        std::lock_guard<std::mutex> lock(userTables->Mutex);

        Tables::iterator it;
        bool inserted;

        std::tie(it, inserted) = userTables->UserTables.emplace(
            name,
            std::move(storage));

        return it->second;
    }
}

////////////////////////////////////////////////////////////////////////////////

std::string TDatabase::getEngineName() const
{
    return "YT";
}

void TDatabase::loadTables(
    Context& context,
    ThreadPool* thread_pool,
    bool hasForceRestoreDataFlag)
{
    // nothing to do
}

bool TDatabase::isTableExist(
    const Context& context,
    const std::string& name) const
{
    auto userTables = GetUserTables(context);

    return GetTable(context, userTables, name) != nullptr;
}

StoragePtr TDatabase::tryGetTable(
    const Context& context,
    const std::string& name) const
{
    auto userTables = GetUserTables(context);

    return GetTable(context, userTables, name);
}

DatabaseIteratorPtr TDatabase::getIterator(const Context& context)
{
    auto token = CreateAuthToken(*Storage, context);

    auto tables = Storage->ListTables(*token);

    return std::make_unique<TDatabaseIterator>(std::move(tables));
}

bool TDatabase::empty(const Context& context) const
{
    // it is too expensive to check
    return false;
}

void TDatabase::createTable(
    const Context& context,
    const std::string& name,
    const StoragePtr& table,
    const ASTPtr& query)
{
    throw Exception(
        "TDatabase: createTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

void TDatabase::removeTable(
    const Context& context,
    const std::string& name)
{
    throw Exception(
        "TDatabase: removeTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

void TDatabase::attachTable(
    const std::string& name,
    const StoragePtr& table)
{
    throw Exception(
        "TDatabase: attachTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

StoragePtr TDatabase::detachTable(const std::string& name)
{
    throw Exception(
        "TDatabase: detachTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

void TDatabase::renameTable(
    const Context& context,
    const std::string& name,
    IDatabase& newDatabase,
    const std::string& newName)
{
    throw Exception(
        "TDatabase: renameTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

void TDatabase::alterTable(
    const Context& context,
    const std::string& name,
    const ColumnsDescription & columns,
    const ASTModifier& engineModifier)
{
    throw Exception(
        "TDatabase: alterTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

time_t TDatabase::getTableMetadataModificationTime(
    const Context& context,
    const std::string& name)
{
    // have no idea what is that
    return 0;
}

ASTPtr TDatabase::getCreateTableQuery(
    const Context& context,
    const std::string& name) const
{
    throw Exception(
        "TDatabase: getCreateTableQuery() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

ASTPtr TDatabase::tryGetCreateTableQuery(
    const Context& context,
    const std::string& name) const
{
    throw Exception(
        "TDatabase: tryGetCreateTableQuery() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

ASTPtr TDatabase::getCreateDatabaseQuery(
    const Context &) const
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

DatabasePtr CreateDatabase(
    NNative::IStoragePtr storage,
    IExecutionClusterPtr cluster)
{
    return std::make_shared<TDatabase>(
        std::move(storage),
        std::move(cluster));
}

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
