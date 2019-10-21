#include "dictionary_source.h"

#include "db_helpers.h"
#include "bootstrap.h"
#include "helpers.h"
#include "private.h"
#include "revision_tracker.h"
#include "block_input_stream.h"

#include <yt/client/ypath/rich.h>

#include <yt/client/table_client/name_table.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/table_reader.h>

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>

#include <Common/Exception.h>
#include <DataStreams/IBlockInputStream.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionaryStructure.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace NYT::NClickHouseServer {

using DB::Exception;

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TTableDictionarySource
    : public DB::IDictionarySource
{
public:
    TTableDictionarySource(
        TBootstrap* bootstrap,
        DB::DictionaryStructure dictionaryStructure,
        TRichYPath path,
        DB::NamesAndTypesList columns)
        : Bootstrap_(bootstrap)
        , DictionaryStructure_(std::move(dictionaryStructure))
        , Path_(std::move(path))
        , Columns_(std::move(columns))
        , RevisionTracker_(path.GetPath(), bootstrap->GetRootClient())
        , Logger(TLogger(ServerLogger)
            .AddTag("Path: %v", Path_))
    { }

    virtual DB::BlockInputStreamPtr loadAll() override
    {
        RevisionTracker_.FixCurrentRevision();

        YT_LOG_INFO("Reloading dictionary (Revision: %v)", RevisionTracker_.GetRevision());

        // We have no information about the user here. It may be a weak point.
        // Anyway, only table's schema is used here, no other information is under threat.
        auto table = FetchClickHouseTableFromCache(Bootstrap_, /* user */ std::nullopt, Path_, Logger);
        if (!table) {
            THROW_ERROR_EXCEPTION("Underlying dictionary table %v does not exist", Path_);
        }

        ValidateStructure(*table);

        auto result = WaitFor(
            NApi::NNative::CreateSchemalessMultiChunkReader(
                Bootstrap_->GetRootClient(),
                Path_,
                NApi::TTableReaderOptions {
                    .EnableTableIndex = false,
                    .EnableRowIndex = false,
                    .EnableRangeIndex = false
                },
                NTableClient::TNameTable::FromSchema(table->TableSchema),
                NTableClient::TColumnFilter(table->TableSchema.Columns().size())))
            .ValueOrThrow();

        return CreateBlockInputStream(result.Reader, table->TableSchema, Logger);
    }

    virtual DB::BlockInputStreamPtr loadIds(const std::vector<UInt64>& /* ids */) override
    {
        throw Exception(
            "Method loadIds is not supported for TableDictionarySource",
            DB::ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual bool supportsSelectiveLoad() const override
    {
        return false;
    }

    virtual DB::BlockInputStreamPtr loadKeys(
        const DB::Columns& /* keyColumns */,
        const std::vector<size_t>& /* requestedRows */) override
    {
        throw Exception(
            "Method loadKeys is not supported for TableDictionarySource",
            DB::ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual bool isModified() const override
    {
        YT_LOG_DEBUG("Checking dictionary revision (OldRevision: %llx)", RevisionTracker_.GetRevision());
        return RevisionTracker_.HasRevisionChanged();
    }

    virtual DB::DictionarySourcePtr clone() const override
    {
        return std::make_unique<TTableDictionarySource>(
            Bootstrap_,
            DictionaryStructure_,
            Path_,
            Columns_);
    }

    std::string toString() const override
    {
        return "YT: " + ToString(Path_);
    }

    DB::BlockInputStreamPtr loadUpdatedAll() override
    {
        throw Exception{"Method loadUpdatedAll is unsupported for TTableDictionarySource", DB::ErrorCodes::NOT_IMPLEMENTED};
    }

    bool hasUpdateField() const override
    {
        return false;
    }

private:
    TBootstrap* Bootstrap_;
    DB::DictionaryStructure DictionaryStructure_;
    TRichYPath Path_;
    DB::NamesAndTypesList Columns_;
    TRevisionTracker RevisionTracker_;
    TLogger Logger;

    void ValidateStructure(const TClickHouseTable& table)
    {
        const auto tableColumns = GetTableColumns(table);

        if (tableColumns != Columns_) {
            throw Exception(
                "table schema does not match dictionary structure: "
                "expected " + Columns_.toString() + ", found " + tableColumns.toString(),
                DB::ErrorCodes::INCOMPATIBLE_COLUMNS);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableDictionarySource(TBootstrap* bootstrap)
{
    auto creator = [bootstrap] (
        const DB::DictionaryStructure& dictionaryStructure,
        const Poco::Util::AbstractConfiguration& config,
        const std::string& dictSectionPath,
        DB::Block& sampleBlock,
        const DB::Context& /* context */) -> DB::DictionarySourcePtr
    {
        const auto& path = TRichYPath::Parse(TString(config.getString(dictSectionPath + ".yt.path")));
        return std::make_unique<TTableDictionarySource>(
            bootstrap,
            dictionaryStructure,
            path,
            sampleBlock.getNamesAndTypesList());
    };

    DB::DictionarySourceFactory::instance().registerSource("yt", creator);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
