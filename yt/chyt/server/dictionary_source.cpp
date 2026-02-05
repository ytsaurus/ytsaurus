#include "dictionary_source.h"

#include "conversion.h"
#include "secondary_query_source.h"
#include "host.h"
#include "query_context.h"
#include "read_plan.h"
#include "revision_tracker.h"
#include "table.h"
#include "helpers.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/table_read_spec.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/table_client/name_table.h>

#include <Common/Exception.h>

#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExternalQueryBuilder.h>

#include <Interpreters/executeQuery.h>
#include <Interpreters/Session.h>

#include <QueryPipeline/QueryPipeline.h>

#include <DBPoco/Util/AbstractConfiguration.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NYPath;
using namespace NLogging;
using namespace NConcurrency;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TTableDictionarySource
    : public DB::IDictionarySource
{
public:
    TTableDictionarySource(
        THost* host,
        DB::DictionaryStructure dictionaryStructure,
        TRichYPath path,
        DB::NamesAndTypesList namesAndTypesList,
        DB::ContextPtr context)
        : Host_(host)
        , DictionaryStructure_(std::move(dictionaryStructure))
        , Path_(std::move(path))
        , NamesAndTypesList_(std::move(namesAndTypesList))
        , RevisionTracker_(path.GetPath(), host->GetRootClient())
        , Logger(ClickHouseYtLogger().WithTag("Path: %v", Path_))
        , Context_(context)
        , QueryBuilder_(std::make_shared<DB::ExternalQueryBuilder>(
            DictionaryStructure_,
            /*db*/ "",
            /*schema*/ "",
            Path_.GetPath(),
            /*query_*/ "",
            /*where_*/ "",
            DB::IdentifierQuotingStyle::Backticks))
        , Session_(std::make_shared<DB::Session>(Host_->GetContext(), DB::ClientInfo::Interface::LOCAL))
    {
        RegisterNewUser(
            Host_->GetContext()->getAccessControl(),
            CacheUserName,
            Host_->GetUserDefinedDatabaseNames(),
            Host_->HasUserDefinedSqlObjectStorage());
        SupportsSelectiveLoad_ = FetchTable()->IsOrderedDynamic();
    }

    DB::QueryPipeline loadAll() override
    {
        RevisionTracker_.FixCurrentRevision();

        YT_LOG_INFO("Reloading dictionary (Revision: %llx)", RevisionTracker_.GetRevision());

        auto table = FetchTable();

        ValidateSchema(*table->Schema);

        auto tableReadSpec = FetchSingleTableReadSpec(TFetchSingleTableReadSpecOptions{
            .RichPath = Path_,
            .Client = Host_->GetRootClient(),
            .GetUserObjectBasicAttributesOptions = {
                .OmitInaccessibleRows = true,
            },
        });

        auto chunkReaderHost = TChunkReaderHost::FromClient(Host_->GetRootClient());
        auto reader = CreateAppropriateSchemalessMultiChunkReader(
            New<TTableReaderOptions>(),
            New<TTableReaderConfig>(),
            std::move(chunkReaderHost),
            tableReadSpec,
            TClientChunkReadOptions(),
            true,
            NTableClient::TNameTable::FromSchema(*table->Schema),
            NTableClient::TColumnFilter(table->Schema->GetColumnCount()));

        auto source = CreateSecondaryQuerySource(
            reader,
            BuildSimpleReadPlan(table->Schema->Columns(), /*columnAttributes*/ {}),
            /*traceContext*/ nullptr,
            Host_,
            Host_->GetConfig()->QuerySettings,
            Logger,
            /*chunkReaderStatistics*/ nullptr,
            /*statisticsCallback*/ {});

        return DB::QueryPipeline(DB::Pipe(source));
    }

    DB::QueryPipeline loadIds(const std::vector<UInt64>& ids) override
    {
        TString query = QueryBuilder_->composeLoadIdsQuery(ids);
        return DB::executeQuery(query, GetContext(), DB::QueryFlags{.internal = true}).second.pipeline;
    }

    bool supportsSelectiveLoad() const override
    {
        return SupportsSelectiveLoad_;
    }

    DB::QueryPipeline loadKeys(
        const DB::Columns& keyColumns,
        const std::vector<size_t>& requestedRows) override
    {
        TString query = QueryBuilder_->composeLoadKeysQuery(keyColumns, requestedRows, DB::ExternalQueryBuilder::AND_OR_CHAIN);
        return DB::executeQuery(query, GetContext(), DB::QueryFlags{.internal = true}).second.pipeline;
    }

    bool isModified() const override
    {
        YT_LOG_DEBUG("Checking dictionary revision (OldRevision: %llx)", RevisionTracker_.GetRevision());
        return RevisionTracker_.HasRevisionChanged();
    }

    DB::DictionarySourcePtr clone() const override
    {
        return std::make_unique<TTableDictionarySource>(
            Host_,
            DictionaryStructure_,
            Path_,
            NamesAndTypesList_,
            Context_);
    }

    std::string toString() const override
    {
        return "YT: " + ToString(Path_);
    }

    DB::QueryPipeline loadUpdatedAll() override
    {
        TString query = QueryBuilder_->composeLoadAllQuery();
        return DB::executeQuery(query, GetContext(), DB::QueryFlags{.internal = true}).second.pipeline;
    }

    bool hasUpdateField() const override
    {
        return false;
    }

private:
    THost* Host_;
    DB::DictionaryStructure DictionaryStructure_;
    TRichYPath Path_;
    DB::NamesAndTypesList NamesAndTypesList_;
    TRevisionTracker RevisionTracker_;
    TLogger Logger;
    DB::ContextPtr Context_;
    DB::ExternalQueryBuilderPtr QueryBuilder_;
    bool SupportsSelectiveLoad_;
    std::shared_ptr<DB::Session> Session_;

    void ValidateSchema(const TTableSchema& schema)
    {
        auto namesAndTypesList = ToNamesAndTypesList(schema, /*columnAttributes*/ {}, New<TCompositeSettings>());
        if (namesAndTypesList != NamesAndTypesList_) {
            THROW_ERROR_EXCEPTION("Dictionary table schema does not match schema from config")
                << TErrorAttribute("config_schema", NamesAndTypesList_.toString())
                << TErrorAttribute("actual_schema", namesAndTypesList.toString());
        }
    }

    TTablePtr FetchTable() {
        auto fakeQueryContext = TQueryContext::CreateFake(Host_, Host_->GetRootClient());

        auto table = FetchTablesSoft(
            fakeQueryContext.Get(),
            {Path_},
            /*skipUnsuitableNodes*/ false,
            /*enableDynamicStoreRead*/ true,
            Logger).front();

        return table;
    }

    DB::ContextMutablePtr GetContext() {
        auto context =  PrepareContextForQuery(
            Session_,
            CacheUserName,
            TDuration::Seconds(0),
            Host_,
            "ChytDictionarySource");
        const auto* queryContext = GetQueryContext(context);
        // We don't want to distribute this query so we set SelectPolicy to local.
        queryContext->SessionSettings->Execution->SelectPolicy = ESelectPolicy::Local;
        return context;
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableDictionarySource(THost* host)
{
    auto creator = [host] (
        const DB::DictionaryStructure& dictionaryStructure,
        const DBPoco::Util::AbstractConfiguration& config,
        const std::string& dictSectionPath,
        DB::Block& sampleBlock,
        DB::ContextPtr context,
        const std::string& /*default_database*/,
        bool /*checkConfig*/) -> DB::DictionarySourcePtr
    {
        const auto& path = TRichYPath::Parse(TString(config.getString(dictSectionPath + ".yt.path")));
        return std::make_unique<TTableDictionarySource>(
            host,
            dictionaryStructure,
            path,
            sampleBlock.getNamesAndTypesList(),
            context);
    };

    DB::DictionarySourceFactory::instance().registerSource("yt", creator);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
