#include "yt_database.h"

#include "storage_distributor.h"
#include "helpers.h"
#include "query_context.h"
#include "query_registry.h"
#include "table.h"
#include "host.h"
#include "table_traverser.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/convert.h>

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <memory>
#include <string>
#include <vector>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

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
        const DB::ContextPtr /*context*/,
        const std::string& /*name*/,
        const DB::StoragePtr& /*table*/,
        const DB::ASTPtr& /*query*/) override
    {
        // Table already created, nothing to do here.
    }

    void shutdown() override
    {
        // Do nothing.
    }

    bool isTableExist(const String& name, DB::ContextPtr context) const override
    {
        return DoGetTable(context, name) != nullptr;
    }

    DB::StoragePtr tryGetTable(const String& name, DB::ContextPtr context) const override
    {
        return DoGetTable(context, name);
    }

    DB::DatabaseTablesIteratorPtr getTablesIterator(
        DB::ContextPtr context,
        const FilterByNameFunction& filterByTableName) const override
    {
        class TTableIterator
            : public DB::IDatabaseTablesIterator
        {
        public:
            TTableIterator(std::vector<String> paths)
                : IDatabaseTablesIterator("YT")
                , Paths_(std::move(paths))
            { }

            bool isValid() const override
            {
                return Index_ < Paths_.size();
            }

            void next() override
            {
                ++Index_;
            }

            const String& name() const override
            {
                return Paths_[Index_];
            }

            DB::StoragePtr& table() const override
            {
                return Table_;
            }

        private:
            const std::vector<String> Paths_;

            size_t Index_ = 0;
            mutable DB::StoragePtr Table_ = nullptr;
        };

        auto* queryContext = GetQueryContext(context);

        TTableTraverser traverser(
            queryContext->Client(),
            *queryContext->Settings->CypressReadOptions,
            queryContext->Host->GetConfig()->ShowTables->Roots,
            filterByTableName);

        return std::make_unique<TTableIterator>(traverser.GetTables());
    }

    bool empty() const override
    {
        // TODO(max42): what does this affect?
        return false;
    }

    bool canContainMergeTreeTables() const override
    {
        return false;
    }

    bool canContainDistributedTables() const override
    {
        return false;
    }

    DB::ASTPtr getCreateDatabaseQuery() const override
    {
        THROW_ERROR_EXCEPTION("Getting CREATE DATABASE query is not supported");
    }

    void dropTable(DB::ContextPtr context, const String& name, bool /*noDelay*/) override
    {
        auto* queryContext = GetQueryContext(context);
        auto path = TYPath(name);

        WaitFor(queryContext->Client()->RemoveNode(path))
            .ThrowOnError();

        InvalidateCache(queryContext, {path});
    }

    void renameTable(
        DB::ContextPtr context,
        const String& name,
        IDatabase& /*toDatabase*/,
        const String& toName,
        bool exchange,
        bool dictionary) override
    {
        if (dictionary) {
            THROW_ERROR_EXCEPTION("Renaming dictionaries is not supported");
        }

        auto* queryContext = GetQueryContext(context);
        auto client = queryContext->Client();
        auto srcPath = TYPath(name);
        auto dstPath = TYPath(toName);

        const auto& Logger = ClickHouseYtLogger;
        YT_LOG_DEBUG("Renaming table (SrcPath: %v, DstPath: %v, Exchange: %v)", srcPath, dstPath, exchange);

        if (exchange) {
            auto transaction = WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Master))
                .ValueOrThrow();
            auto tmpPath = TYPath(Format("//tmp/tmp_exchange_table_%v", transaction->GetId()));

            WaitFor(transaction->MoveNode(srcPath, tmpPath))
                .ThrowOnError();
            WaitFor(transaction->MoveNode(dstPath, srcPath))
                .ThrowOnError();
            WaitFor(transaction->MoveNode(tmpPath, dstPath))
                .ThrowOnError();

            WaitFor(transaction->Commit())
                .ThrowOnError();
        } else {
            WaitFor(client->MoveNode(srcPath, dstPath))
                .ThrowOnError();
        }

        InvalidateCache(queryContext, {srcPath, dstPath});
    }

    DB::ASTPtr getCreateTableQueryImpl(const String& name, DB::ContextPtr context, bool throwOnError) const override
    {
        auto* queryContext = GetQueryContext(context);
        auto path = TRichYPath::Parse(TString(name));

        TGetNodeOptions options;
        static_cast<TMasterReadOptions&>(options) = *queryContext->Settings->CypressReadOptions;
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
            false /*hilite*/,
            "(n/a)",
            false /*allow_multi_statements*/,
            0 /*max_query_size*/,
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
        // Definitely not a YT table. Don't even try to parse it.
        if (!name.starts_with("//") && !name.starts_with("<")) {
            return nullptr;
        }

        // Normally it's called with a query context.
        // In rare cases CH tries to find special tables (e.g. Dictionary)
        // outside of query execution and provides a global context.
        // We do not support such tables, so returning nullptr is fine.
        if (context->isGlobalContext()) {
            return nullptr;
        }

        auto* queryContext = GetQueryContext(context);

        try {
            auto tables = FetchTables(
                queryContext,
                {TRichYPath::Parse(name.data())},
                /*skipUnsuitableNodes*/ false,
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
