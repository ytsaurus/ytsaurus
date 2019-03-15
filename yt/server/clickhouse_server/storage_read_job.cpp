#include "storage_read_job.h"

#include "db_helpers.h"
#include "format_helpers.h"
#include "input_stream.h"
#include "storage_with_virtual_columns.h"
#include "type_helpers.h"
#include "virtual_columns.h"

#include "query_context.h"
#include "table_reader.h"
#include "table.h"

#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TStorageReadJob
    : public IStorageWithVirtualColumns
{
private:
    TQueryContext* QueryContext_;
    const NamesAndTypesList Columns;
    const std::string JobSpec;

public:
    TStorageReadJob(
        TQueryContext* queryContext,
        NamesAndTypesList columns,
        std::string jobSpec)
        : QueryContext_(queryContext)
        , Columns(std::move(columns))
        , JobSpec(std::move(jobSpec))
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
        QueryProcessingStage::Enum processedStage,
        size_t maxBlockSize,
        unsigned numStreams) override;

    QueryProcessingStage::Enum getQueryProcessingStage(const Context& context) const override;

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
    const SelectQueryInfo& /* queryInfo */,
    const Context& /* context */,
    QueryProcessingStage::Enum /* processedStage */,
    size_t /* maxBlockSize */,
    unsigned numStreams)
{
    DB::Names physicalColumns;
    DB::Names virtualColumns;
    SplitColumns(columnNames, physicalColumns, virtualColumns);

    auto tableReaders = QueryContext_->CreateTableReaders(
        ToString(JobSpec),
        ToString(physicalColumns),
        GetSystemColumns(virtualColumns),
        numStreams,
        true /* unordered */);

    BlockInputStreams streams;
    for (auto& tableReader: tableReaders) {
        streams.emplace_back(CreateStorageInputStream(std::move(tableReader)));
    }

    return streams;
}

QueryProcessingStage::Enum TStorageReadJob::getQueryProcessingStage(const Context& /* context */) const
{
    return QueryProcessingStage::Enum::FetchColumns;
}

////////////////////////////////////////////////////////////////////////////////

StoragePtr CreateStorageReadJob(
    TQueryContext* queryContext,
    std::vector<TClickHouseTablePtr> tables,
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
        queryContext,
        std::move(columns),
        std::move(jobSpec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
