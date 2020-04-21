#include "database.h"

#include "storage_distributor.h"
#include "helpers.h"
#include "query_context.h"
#include "query_registry.h"
#include "table.h"

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

////////////////////////////////////////////////////////////////////////////////

class TDatabase
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

StoragePtr TDatabase::GetTable(
    const Context& context,
    const std::string& path) const
{
    auto* queryContext = GetQueryContext(context);

    // Here goes the dirty-ass hack. When query context is created, query AST is not parsed yet,
    // so it is not present in client info for query. That's why if we crash somewhere during coordination
    // phase, dumped query registry in crash handler will lack crashing query itself. As a workaround,
    // we forcefully rebuild query registry state when creating TStorageDistributor.
    WaitFor(
        BIND(&TQueryRegistry::SaveState, queryContext->Bootstrap->GetQueryRegistry())
            .AsyncVia(queryContext->Bootstrap->GetControlInvoker())
            .Run())
        .ThrowOnError();

    try {
        auto tables = FetchTables(
            queryContext->Client(),
            queryContext->Bootstrap->GetHost(),
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

std::string TDatabase::getEngineName() const
{
    return "YT";
}

void TDatabase::loadTables(
    Context& /* context */,
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

DatabaseIteratorPtr TDatabase::getIterator(const Context& /* context */, const FilterByNameFunction & /* filter_by_table_name */)
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
    // Table already created, nothing to do here.
}

void TDatabase::removeTable(
    const Context&  context,
    const std::string& name)
{
    auto* queryContext = GetQueryContext(context);
    WaitFor(queryContext->Client()->RemoveNode(TYPath(name)))
        .ThrowOnError();
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
    const IndicesDescription & /* indices */,
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

ASTPtr TDatabase::getCreateTableQuery(const Context& context, const std::string& name) const
{
    return DoGetCreateTableQuery(context, name, true /* throwOnError */);
}

ASTPtr TDatabase::tryGetCreateTableQuery(const Context& context, const std::string& name) const
{
    return DoGetCreateTableQuery(context, name, false /* throwOnError */);
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

ASTPtr TDatabase::DoGetCreateTableQuery(const DB::Context& context, const DB::String& tableName, bool throwOnError) const
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
    auto result = WaitFor(queryContext->Client()->GetNode(path.GetPath(), options))
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

DatabasePtr CreateDatabase()
{
    return std::make_shared<TDatabase>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
