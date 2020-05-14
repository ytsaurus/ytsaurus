#include "dictionary_source.h"

#include "helpers.h"
#include "table.h"
#include "host.h"
#include "schema.h"
#include "revision_tracker.h"
#include "block_input_stream.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/table_reader.h>

#include <yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/client/ypath/rich.h>

#include <yt/client/table_client/name_table.h>

#include <Common/Exception.h>
#include <DataStreams/IBlockInputStream.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionaryStructure.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NYPath;
using namespace NLogging;
using namespace NConcurrency;

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
        , Logger(TLogger(ClickHouseYtLogger)
            .AddTag("Path: %v", Path_))
    { }

    virtual DB::BlockInputStreamPtr loadAll() override
    {
        RevisionTracker_.FixCurrentRevision();

        YT_LOG_INFO("Reloading dictionary (Revision: %v)", RevisionTracker_.GetRevision());

        auto table = FetchTables(
            Host_->GetRootClient(),
            Host_,
            {Path_},
            /* skipUnsuitableNodes */ false,
            Logger).front();

        ValidateSchema(table->Schema);

        auto result = WaitFor(
            NApi::NNative::CreateSchemalessMultiChunkReader(
                Host_->GetRootClient(),
                Path_,
                NApi::TTableReaderOptions(),
                NTableClient::TNameTable::FromSchema(table->Schema),
                NTableClient::TColumnFilter(table->Schema.GetColumnCount())))
            .ValueOrThrow();

        return CreateBlockInputStream(result.Reader, table->Schema, nullptr /* traceContext */, Host_, Logger, nullptr /* prewhereInfo */);
    }

    virtual DB::BlockInputStreamPtr loadIds(const std::vector<UInt64>& /* ids */) override
    {
        THROW_ERROR_EXCEPTION("Method loadIds not supported");
    }

    virtual bool supportsSelectiveLoad() const override
    {
        return false;
    }

    virtual DB::BlockInputStreamPtr loadKeys(
        const DB::Columns& /* keyColumns */,
        const std::vector<size_t>& /* requestedRows */) override
    {
        THROW_ERROR_EXCEPTION("Method loadKeys not supported");
    }

    virtual bool isModified() const override
    {
        YT_LOG_DEBUG("Checking dictionary revision (OldRevision: %llx)", RevisionTracker_.GetRevision());
        return RevisionTracker_.HasRevisionChanged();
    }

    virtual DB::DictionarySourcePtr clone() const override
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

    DB::BlockInputStreamPtr loadUpdatedAll() override
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
        auto namesAndTypesList = ToNamesAndTypesList(schema);
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
        const DB::Context& /* context */) -> DB::DictionarySourcePtr
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
