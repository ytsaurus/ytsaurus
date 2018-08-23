#include "table_dictionary_source.h"

#include "auth_token.h"
#include "db_helpers.h"
#include "input_stream.h"
#include "logging_helpers.h"
#include "type_helpers.h"
#include "updates_tracker.h"

#include <Common/Exception.h>
#include <DataStreams/IBlockInputStream.h>
#include <Dictionaries/DictionarySourceFactory.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCOMPATIBLE_COLUMNS;
}

}

namespace NYT {
namespace NClickHouse {

using DB::Exception;

////////////////////////////////////////////////////////////////////////////////

// TODO: support column filtration and schema validation

class TTableDictionarySource
    : public DB::IDictionarySource
{
private:
    NInterop::IStoragePtr Storage;
    NInterop::IAuthorizationTokenPtr Token;
    std::string TableName;
    DB::NamesAndTypesList Columns;

    IUpdatesTrackerPtr UpdatesTracker;

public:
    TTableDictionarySource(
        NInterop::IStoragePtr storage,
        NInterop::IAuthorizationTokenPtr token,
        std::string name,
        DB::NamesAndTypesList columns);

    DB::BlockInputStreamPtr loadAll() override;

    bool supportsSelectiveLoad() const override;

    DB::BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    DB::BlockInputStreamPtr loadKeys(
        const DB::Columns& keyColumns,
        const std::vector<size_t>& requestedRows) override;

    bool isModified() const override;

    DB::DictionarySourcePtr clone() const override;

    std::string toString() const override;

    DB::BlockInputStreamPtr loadUpdatedAll() override
    {
        throw Exception{"Method loadUpdatedAll is unsupported for TTableDictionarySource", DB::ErrorCodes::NOT_IMPLEMENTED};
    }

    bool hasUpdateField() const override { return false; }

private:
    void ValidateStructure(const NInterop::TTable& table);
};

////////////////////////////////////////////////////////////////////////////////

TTableDictionarySource::TTableDictionarySource(NInterop::IStoragePtr storage,
                                               NInterop::IAuthorizationTokenPtr token,
                                               std::string name,
                                               DB::NamesAndTypesList columns)
   : Storage(std::move(storage))
   , Token(std::move(token))
   , TableName(std::move(name))
   , Columns(std::move(columns))
   , UpdatesTracker(CreateUpdatesTracker(Storage, Token, TableName))
{
}

DB::BlockInputStreamPtr TTableDictionarySource::loadAll()
{
    // TODO: support column filtering and reordering

    UpdatesTracker->FixCurrentVersion();

    auto table = Storage->GetTable(*Token, ToString(TableName));
    ValidateStructure(*table);

    NInterop::TTableReaderOptions readerOptions;
    readerOptions.Unordered = false;

    auto reader = Storage->CreateTableReader(*Token, ToString(TableName), readerOptions);
    return CreateStorageInputStream(std::move(reader));
}

bool TTableDictionarySource::supportsSelectiveLoad() const
{
    return false;
}

DB::BlockInputStreamPtr TTableDictionarySource::loadIds(const std::vector<UInt64>& ids)
{
    throw Exception(
        "Method loadIds is not supported for TableDictionarySource",
        DB::ErrorCodes::NOT_IMPLEMENTED);
}

DB::BlockInputStreamPtr TTableDictionarySource::loadKeys(
    const DB::Columns& keyColumns,
    const std::vector<size_t>& requestedRows)
{
    throw Exception(
        "Method loadKeys is not supported for TableDictionarySource",
        DB::ErrorCodes::NOT_IMPLEMENTED);
}

bool TTableDictionarySource::isModified() const
{
    return UpdatesTracker->IsModified();
}

DB::DictionarySourcePtr TTableDictionarySource::clone() const
{
    return std::make_unique<TTableDictionarySource>(
        Storage,
        Token,
        TableName,
        Columns);
}

std::string TTableDictionarySource::toString() const
{
    return "Table " + TableName;
}

void TTableDictionarySource::ValidateStructure(const NInterop::TTable& table)
{
    const auto tableColumns = GetTableColumns(table);

    if (tableColumns != Columns) {
        throw Exception(
            "table schema does not match dictionary structure: "
            "expected " + Columns.toString() + ", found " + tableColumns.toString(),
            DB::ErrorCodes::INCOMPATIBLE_COLUMNS);
    }
}

////////////////////////////////////////////////////////////////////////////////

DB::DictionarySourcePtr CreateTableDictionarySource(
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr authToken,
    const std::string& tableName,
    const DB::Block& sampleBlock)
{
    return std::make_unique<TTableDictionarySource>(
        std::move(storage),
        std::move(authToken),
        tableName,
        sampleBlock.getNamesAndTypesList());
}

////////////////////////////////////////////////////////////////////////////////

void RegisterTableDictionarySource(
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr authToken)
{
    auto createTableSource = [=] (
        const DB::DictionaryStructure& dictStructure,
        const Poco::Util::AbstractConfiguration& config,
        const std::string& dictSectionPath,
        DB::Block& sampleBlock,
        const DB::Context& context) -> DB::DictionarySourcePtr
    {
        const auto tableName = config.getString(dictSectionPath + ".yt_table.path");
        return CreateTableDictionarySource(storage, authToken, tableName, sampleBlock);
    };

    DB::DictionarySourceFactory::instance().registerSource("yt_table", createTableSource);
}

} // namespace NClickHouse
} // namespace NYT
