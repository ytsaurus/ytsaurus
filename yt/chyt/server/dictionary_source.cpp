#include "dictionary_source.h"

#include "helpers.h"
#include "table.h"
#include "host.h"
#include "conversion.h"
#include "revision_tracker.h"
#include "block_input_stream.h"
#include "query_context.h"

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
#include <Processors/Sources/SourceFromInputStream.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Poco/Util/AbstractConfiguration.h>

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
        DB::NamesAndTypesList namesAndTypesList)
        : Host_(host)
        , DictionaryStructure_(std::move(dictionaryStructure))
        , Path_(std::move(path))
        , NamesAndTypesList_(std::move(namesAndTypesList))
        , RevisionTracker_(path.GetPath(), host->GetRootClient())
        , Logger(ClickHouseYtLogger.WithTag("Path: %v", Path_))
    { }

    DB::QueryPipeline loadAll() override
    {
        RevisionTracker_.FixCurrentRevision();

        YT_LOG_INFO("Reloading dictionary (Revision: %llx)", RevisionTracker_.GetRevision());

        auto fakeQueryContext = TQueryContext::CreateFake(Host_, Host_->GetRootClient());

        auto table = FetchTables(
            fakeQueryContext.Get(),
            {Path_},
            /*skipUnsuitableNodes*/ false,
            /*enableDynamicStoreRead*/ true,
            Logger).front();

        ValidateSchema(*table->Schema);

        auto tableReadSpec = FetchSingleTableReadSpec(TFetchSingleTableReadSpecOptions{
            .RichPath = Path_,
            .Client = Host_->GetRootClient(),
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

        auto blockInputStream = CreateBlockInputStream(
            reader,
            table->Schema,
            nullptr /*traceContext*/,
            Host_,
            Host_->GetConfig()->QuerySettings,
            Logger,
            nullptr /*prewhereInfo*/);

        auto source = std::make_shared<DB::SourceFromInputStream>(std::move(blockInputStream));

        return DB::QueryPipeline(DB::Pipe(source));
    }

    DB::QueryPipeline loadIds(const std::vector<UInt64>& /*ids*/) override
    {
        THROW_ERROR_EXCEPTION("Method loadIds not supported");
    }

    bool supportsSelectiveLoad() const override
    {
        return false;
    }

    DB::QueryPipeline loadKeys(
        const DB::Columns& /*keyColumns*/,
        const std::vector<size_t>& /*requestedRows*/) override
    {
        THROW_ERROR_EXCEPTION("Method loadKeys not supported");
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
            NamesAndTypesList_);
    }

    std::string toString() const override
    {
        return "YT: " + ToString(Path_);
    }

    DB::QueryPipeline loadUpdatedAll() override
    {
        THROW_ERROR_EXCEPTION("Method loadUpdatedAll not supported");
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

    void ValidateSchema(const TTableSchema& schema)
    {
        auto namesAndTypesList = ToNamesAndTypesList(schema, New<TCompositeSettings>());
        if (namesAndTypesList != NamesAndTypesList_) {
            THROW_ERROR_EXCEPTION("Dictionary table schema does not match schema from config")
                << TErrorAttribute("config_schema", NamesAndTypesList_.toString())
                << TErrorAttribute("actual_schema", namesAndTypesList.toString());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableDictionarySource(THost* host)
{
    auto creator = [host] (
        const DB::DictionaryStructure& dictionaryStructure,
        const Poco::Util::AbstractConfiguration& config,
        const std::string& dictSectionPath,
        DB::Block& sampleBlock,
        DB::ContextPtr /*context*/,
        const std::string& /*default_database*/,
        bool /*checkConfig*/) -> DB::DictionarySourcePtr
    {
        const auto& path = TRichYPath::Parse(TString(config.getString(dictSectionPath + ".yt.path")));
        return std::make_unique<TTableDictionarySource>(
            host,
            dictionaryStructure,
            path,
            sampleBlock.getNamesAndTypesList());
    };

    DB::DictionarySourceFactory::instance().registerSource("yt", creator);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
