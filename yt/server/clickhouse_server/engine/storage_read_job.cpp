#include "storage_read_job.h"

#include "auth_token.h"
#include "db_helpers.h"
#include "format_helpers.h"
#include "input_stream.h"
#include "storage_with_virtual_columns.h"
#include "type_helpers.h"
#include "virtual_columns.h"

#include <Interpreters/Context.h>

#include <Poco/Logger.h>

#include <common/logger_useful.h>

namespace DB {

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int LOGICAL_ERROR;
}

}   // namespace DB

namespace NYT {
namespace NClickHouse {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TStorageReadJob
    : public IStorageWithVirtualColumns
{
private:
    const NInterop::IStoragePtr Storage;
    const NamesAndTypesList Columns;
    const std::string JobSpec;

    Poco::Logger* Logger;

public:
    TStorageReadJob(NInterop::IStoragePtr storage,
                    NamesAndTypesList columns,
                    std::string jobSpec)
        : Storage(std::move(storage))
        , Columns(std::move(columns))
        , JobSpec(std::move(jobSpec))
        , Logger(&Poco::Logger::get("StorageReadJob"))
    {
        setColumns(ColumnsDescription(Columns));
    }

    std::string getName() const override { return "YT"; }

    std::string getTableName() const override { return "ReadJob"; }

    bool isRemote() const override { return true; }

    BlockInputStreams read(
        const Names& columnNames,
        const SelectQueryInfo& queryInfo,
        const Context& context,
        QueryProcessingStage::Enum& processedStage,
        size_t maxBlockSize,
        unsigned numStreams) override;

private:
    const NamesAndTypesList& ListPhysicalColumns() const override
    {
        return Columns;
    }

    const NamesAndTypesList& ListVirtualColumns() const override
    {
        return ListSystemVirtualColumns();
    }
};

////////////////////////////////////////////////////////////////////////////////

BlockInputStreams TStorageReadJob::read(
    const Names& columnNames,
    const SelectQueryInfo& queryInfo,
    const Context& context,
    QueryProcessingStage::Enum& processedStage,
    size_t maxBlockSize,
    unsigned numStreams)
{
    LOG_DEBUG(Logger, "Columns requested in read job: " << JoinStrings(",", ToString(columnNames)));

    DB::Names physicalColumns;
    DB::Names virtualColumns;
    SplitColumns(columnNames, physicalColumns, virtualColumns);

    auto token = CreateAuthToken(*Storage, context);

    NInterop::TTableReaderOptions readerOptions;
    readerOptions.Unordered = true;

    auto tableReaders = Storage->CreateTableReaders(
        *token,
        ToString(JobSpec),
        ToString(physicalColumns),
        GetSystemColumns(virtualColumns),
        numStreams,
        readerOptions);

    BlockInputStreams streams;
    for (auto& tableReader: tableReaders) {
        streams.emplace_back(CreateStorageInputStream(std::move(tableReader)));
    }

    return streams;
}

////////////////////////////////////////////////////////////////////////////////

StoragePtr CreateStorageReadJob(
    NInterop::IStoragePtr storage,
    NInterop::TTableList tables,
    std::string jobSpec)
{
    if (tables.empty()) {
        throw Exception(
            "Cannot create table part storage: table list is empty",
            DB::ErrorCodes::LOGICAL_ERROR);
    }

    const auto& representative = tables.front();
    DB::NamesAndTypesList columns = GetTableColumns(*representative);

    return std::make_shared<TStorageReadJob>(
        std::move(storage),
        std::move(columns),
        std::move(jobSpec));
}

}   // namespace NClickHouse
}   // namespace NYT
