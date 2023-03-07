#include "yt_database.h"

#include "storage_distributor.h"
#include "helpers.h"
#include "query_context.h"
#include "query_registry.h"
#include "table.h"
#include "host.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/client/ypath/rich.h>

#include <yt/core/ytree/convert.h>

#include <Common/Exception.h>
#include <Common/LRUCache.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <memory>
#include <string>
#include <vector>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TYtDatabase
    : public IDatabase
{
public:
    std::string getEngineName() const override;

    void loadTables(
        Context& context,
        bool hasForceRestoreDataFlag) override;

    bool isTableExist(
        const Context& context,
        const std::string& name) const override;

    StoragePtr tryGetTable(
        const Context& context,
        const std::string& name) const override;

    DatabaseIteratorPtr getIterator(const Context & context, const FilterByNameFunction & filter_by_table_name = {}) override;

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
        const IndicesDescription & indices,
        const ASTModifier& engineModifier) override;

    time_t getTableMetadataModificationTime(
        const Context& context,
        const std::string& name) override;

    ASTPtr getCreateTableQuery(const Context & context, const String & table_name) const override;

    ASTPtr tryGetCreateTableQuery(const Context & context, const String & table_name) const override;

    ASTPtr getCreateDatabaseQuery(
        const Context &) const override;

    std::string getDatabaseName() const override;

    void shutdown() override;

    void drop() override;

private:
    StoragePtr GetTable(
        const Context& context,
        const std::string& name) const;

    ASTPtr DoGetCreateTableQuery(const Context& context, const String& tableName, bool throwOnError) const;
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr TYtDatabase::GetTable(
    const Context& context,
    const std::string& path) const
{
    auto* queryContext = GetQueryContext(context);

    // Here goes the dirty-ass hack. When query context is created, query AST is not parsed yet,
    // so it is not present in client info for query. That's why if we crash somewhere during coordination
    // phase, dumped query registry in crash handler will lack crashing query itself. As a workaround,
    // we forcefully rebuild query registry state when creating TStorageDistributor.
    queryContext->Host->SaveQueryRegistryState();

    try {
        auto tables = FetchTables(
            queryContext->Client(),
            queryContext->Host,
            {TRichYPath::Parse(path.data())},
            /* skipUnsuitableNodes */ false,
            queryContext->Logger);

        return CreateStorageDistributor(queryContext, std::move(tables));
    } catch (const TErrorException& ex) {
        if (ex.Error().FindMatching(NYTree::EErrorCode::ResolveError)) {
            return nullptr;
        }
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

std::string TYtDatabase::getEngineName() const
{
    return "YT";
}

void TYtDatabase::loadTables(
    Context& /* context */,
    bool /* hasForceRestoreDataFlag */)
{
    // nothing to do
}

bool TYtDatabase::isTableExist(
    const Context& context,
    const std::string& name) const
{
    return GetTable(context, name) != nullptr;
}

StoragePtr TYtDatabase::tryGetTable(
    const Context& context,
    const std::string& name) const
{
    return GetTable(context, name);
}

DatabaseIteratorPtr TYtDatabase::getIterator(const Context& /* context */, const FilterByNameFunction & /* filter_by_table_name */)
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
            YT_ABORT();
        }

        virtual const String& name() const override
        {
            YT_ABORT();
        }

        virtual StoragePtr& table() const override
        {
            YT_ABORT();
        }
    };

    return std::make_unique<TDummyIterator>();
}

bool TYtDatabase::empty(const Context& /* context */) const
{
    // it is too expensive to check
    return false;
}

void TYtDatabase::createTable(
    const Context& /* context */,
    const std::string& /* name */,
    const StoragePtr& /* table */,
    const ASTPtr& /* query */)
{
    // Table already created, nothing to do here.
}

void TYtDatabase::removeTable(
    const Context&  context,
    const std::string& name)
{
    auto* queryContext = GetQueryContext(context);
    WaitFor(queryContext->Client()->RemoveNode(TYPath(name)))
        .ThrowOnError();
}

void TYtDatabase::attachTable(
    const std::string& /* name */,
    const StoragePtr& /* table */)
{
    throw Exception(
        "TYtDatabase: attachTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

StoragePtr TYtDatabase::detachTable(const std::string& /* name */)
{
    throw Exception(
        "TYtDatabase: detachTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

void TYtDatabase::renameTable(
    const Context& /* context */,
    const std::string& /* name */,
    IDatabase& /* newDatabase */,
    const std::string& /* newName */)
{
    throw Exception(
        "TYtDatabase: renameTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

void TYtDatabase::alterTable(
    const Context& /* context */,
    const std::string& /* name */,
    const ColumnsDescription & /* columns */,
    const IndicesDescription & /* indices */,
    const ASTModifier& /* engineModifier */)
{
    throw Exception(
        "TYtDatabase: alterTable() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

time_t TYtDatabase::getTableMetadataModificationTime(
    const Context& /* context */,
    const std::string& /* name */)
{
    // have no idea what is that
    return 0;
}

ASTPtr TYtDatabase::getCreateTableQuery(const Context& context, const std::string& name) const
{
    return DoGetCreateTableQuery(context, name, true /* throwOnError */);
}

ASTPtr TYtDatabase::tryGetCreateTableQuery(const Context& context, const std::string& name) const
{
    return DoGetCreateTableQuery(context, name, false /* throwOnError */);
}

ASTPtr TYtDatabase::getCreateDatabaseQuery(
    const Context& /* context */) const
{
    throw Exception(
        "TYtDatabase: getCreateDatabaseQuery() is not supported",
        ErrorCodes::NOT_IMPLEMENTED);
}

std::string TYtDatabase::getDatabaseName() const
{
    return {};
};

void TYtDatabase::shutdown()
{
    // nothing to do
}

void TYtDatabase::drop()
{
    // nothing to do
}

ASTPtr TYtDatabase::DoGetCreateTableQuery(const DB::Context& context, const DB::String& tableName, bool throwOnError) const
{
    auto* queryContext = GetQueryContext(context);
    auto path = TRichYPath::Parse(TString(tableName));
    NApi::TGetNodeOptions options;
    options.Attributes = {
        "compression_codec",
        "erasure_codec",
        "replication_factor",
        "optimize_for",
        "schema",
    };
    auto result = NConcurrency::WaitFor(queryContext->Client()->GetNode(path.GetPath(), options))
        .ValueOrThrow();
    auto attributesYson = ConvertToYsonString(
        ConvertToNode(result)->Attributes().ToMap(),
        EYsonFormat::Text);
    auto query = Format("CREATE TABLE \"%v\"(_ UInt8) ENGINE YtTable('%v')", path, attributesYson);

    DB::ParserCreateQuery parser;
    std::string errorMessage;
    const char* data = query.data();
    auto ast = tryParseQuery(
        parser,
        data,
        data + query.size(),
        errorMessage,
        false /* hilite */,
        "(n/a)",
        false /* allow_multi_statements */,
        0 /* max_query_size */);

    if (!ast && throwOnError) {
        THROW_ERROR_EXCEPTION("Caught following error while parsing table creation query: %v", errorMessage);
    }

    return ast;
}

////////////////////////////////////////////////////////////////////////////////

DatabasePtr CreateYtDatabase()
{
    return std::make_shared<TYtDatabase>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
