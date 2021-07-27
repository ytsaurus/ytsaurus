#include "yt_database.h"

#include "storage_distributor.h"
#include "helpers.h"
#include "query_context.h"
#include "query_registry.h"
#include "table.h"
#include "host.h"
#include "table_traverser.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/convert.h>

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

using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TYtDatabase
    : public DB::IDatabase
{
public:
    TYtDatabase()
        : DB::IDatabase("YT")
    { }

    std::string getEngineName() const override
    {
        return "Lazy";
    }

    void createTable(
        const DB::ContextPtr /* context */,
        const std::string& /* name */,
        const DB::StoragePtr& /* table */,
        const DB::ASTPtr& /* query */) override
    {
        // Table already created, nothing to do here.
    }

    void shutdown() override
    {
        // Do nothing.
    }

    virtual bool isTableExist(const String& name, DB::ContextPtr context) const override
    {
        return DoGetTable(context, name) != nullptr;
    }

    virtual DB::StoragePtr tryGetTable(const String& name, DB::ContextPtr context) const override
    {
        return DoGetTable(context, name);
    }

    virtual DB::DatabaseTablesIteratorPtr getTablesIterator(
        DB::ContextPtr context,
        const FilterByNameFunction& filterByTableName) override
    {
        class TTableIterator
            : public DB::IDatabaseTablesIterator
        {
        public:
            TTableIterator(std::vector<String> paths)
                : DB::IDatabaseTablesIterator("YT")
                , Paths_(std::move(paths))
            { }

            virtual bool isValid() const override
            {
                return Index_ < Paths_.size();
            }

            virtual void next() override
            {
                ++Index_;
            }

            virtual const String& name() const override
            {
                return Paths_[Index_];
            }

            virtual DB::StoragePtr& table() const override
            {
                return Table_;
            }

        private:
            const std::vector<String> Paths_;

            size_t Index_ = 0;
            mutable DB::StoragePtr Table_ = nullptr;
        };

        auto* queryContext = GetQueryContext(context);

        TTableTraverser traverser(queryContext->Client(), queryContext->Host->GetConfig()->ShowTables->Roots,  filterByTableName);

        return std::make_unique<TTableIterator>(traverser.GetTables());
    }

    virtual bool empty() const override
    {
        // TODO(max42): what does this affect?
        return false;
    }

    virtual bool canContainMergeTreeTables() const override
    {
        return false;
    }

    virtual bool canContainDistributedTables() const override
    {
        return false;
    }

    virtual DB::ASTPtr getCreateDatabaseQuery() const override
    {
        THROW_ERROR_EXCEPTION("Getting CREATE DATABASE query is not supported");
    }

    virtual void dropTable(DB::ContextPtr context, const String& name, bool /* noDelay */) override
    {
        auto* queryContext = GetQueryContext(context);
        WaitFor(queryContext->Client()->RemoveNode(TYPath(name)))
            .ThrowOnError();
    }

    virtual DB::ASTPtr getCreateTableQueryImpl(const String& name, DB::ContextPtr context, bool throwOnError) const override
    {
        auto* queryContext = GetQueryContext(context);
        auto path = TRichYPath::Parse(TString(name));
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
            0 /* max_query_size */,
            DBMS_DEFAULT_MAX_PARSER_DEPTH);

        if (!ast && throwOnError) {
            THROW_ERROR_EXCEPTION("Caught following error while parsing table creation query: %v", errorMessage);
        }

        return ast;
    }

private:
    DB::StoragePtr DoGetTable(
        DB::ContextPtr context,
        const std::string& name) const
    {
        // Normally it's called with a query context.
        // In rare cases CH tries to find special tables (e.g. Dictionary)
        // outside of query execution and provides a global context.
        // We do not support such tables, so returning nullptr is fine.
        if (context->isGlobalContext()) {
            return nullptr;
        }

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
                {TRichYPath::Parse(name.data())},
                /* skipUnsuitableNodes */ false,
                queryContext->Settings->DynamicTable->EnableDynamicStoreRead,
                queryContext->Logger);

            return CreateStorageDistributor(context, std::move(tables));
        } catch (const TErrorException& ex) {
            if (ex.Error().FindMatching(NYTree::EErrorCode::ResolveError)) {
                return nullptr;
            }
            throw;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::DatabasePtr CreateYtDatabase()
{
    return std::make_shared<TYtDatabase>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
