#pragma once

#include "cluster_tracker.h"
#include "storage_with_virtual_columns.h"
#include "table_schema.h"

#include <yt/server/clickhouse_server/native/table_partition.h>

#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>

#include <Poco/Logger.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

struct TTablePartAllocation
{
    NNative::TTablePart TablePart;
    IClusterNodePtr TargetClusterNode;

    TTablePartAllocation(
        NNative::TTablePart part,
        IClusterNodePtr node)
        : TablePart(std::move(part))
        , TargetClusterNode(std::move(node))
    {
    }
};

using TTableAllocation = std::vector<TTablePartAllocation>;

////////////////////////////////////////////////////////////////////////////////

// Abstract base class for distributed storages

class TStorageDistributed
    : public IStorageWithVirtualColumns
{
private:
    const NNative::IStoragePtr Storage;
    const IExecutionClusterPtr Cluster;
    TTableSchema Schema;

    Poco::Logger* Logger;

public:
    TStorageDistributed(NNative::IStoragePtr storage,
                        IExecutionClusterPtr cluster,
                        TTableSchema schema,
                        Poco::Logger* logger)
        : Storage(std::move(storage))
        , Cluster(std::move(cluster))
        , Schema(std::move(schema))
        , Logger(logger)
    {
        setColumns(DB::ColumnsDescription(ListPhysicalColumns()));
    }

    // Database name
    std::string getName() const override
    {
        return "YT";
    }

    bool isRemote() const override
    {
        return true;
    }

    DB::QueryProcessingStage::Enum getQueryProcessingStage(const DB::Context& context) const override;

    DB::BlockInputStreams read(
        const DB::Names& columnNames,
        const DB::SelectQueryInfo& queryInfo,
        const DB::Context& context,
        DB::QueryProcessingStage::Enum processedStage,
        size_t maxBlockSize,
        unsigned numStreams) override;

protected:
    virtual NNative::TTablePartList GetTableParts(
        const DB::ASTPtr& queryAst,
        const DB::Context& context,
        NNative::IRangeFilterPtr rangeFilter,
        const size_t maxParts) = 0;

    virtual DB::ASTPtr RewriteSelectQueryForTablePart(
        const DB::ASTPtr& queryAst,
        const std::string& jobSpec) = 0;

protected:
    const NNative::IStoragePtr& GetStorage() const
    {
        return Storage;
    }

    const IExecutionClusterPtr& GetCluster() const
    {
        return Cluster;
    }

    const TTableSchema& GetSchema() const
    {
        return Schema;
    }

    Poco::Logger* GetLogger() const
    {
        return Logger;
    }

private:
    const DB::NamesAndTypesList& ListPhysicalColumns() const override
    {
        return Schema.Columns;
    }

    TTableAllocation AllocateTablePartsToClusterNodes(
        const TClusterNodes& clusterNodes,
        const DB::SelectQueryInfo& queryInfo,
        const DB::Context& context);

    NNative::IRangeFilterPtr CreateRangeFilter(
        const DB::SelectQueryInfo& queryInfo,
        const DB::Context& context);

    static DB::Settings PrepareLeafJobSettings(const DB::Settings& settings);

    static DB::ThrottlerPtr CreateNetThrottler(const DB::Settings& settings);

    static DB::BlockInputStreamPtr CreateLocalStream(
        const DB::ASTPtr& queryAst,
        const DB::Context& context,
        DB::QueryProcessingStage::Enum processedStage);

    static DB::BlockInputStreamPtr CreateRemoteStream(
        const IClusterNodePtr remoteNode,
        const DB::ASTPtr& queryAst,
        const DB::Context& context,
        const DB::ThrottlerPtr& throttler,
        const DB::Tables& externalTables,
        DB::QueryProcessingStage::Enum processedStage);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
