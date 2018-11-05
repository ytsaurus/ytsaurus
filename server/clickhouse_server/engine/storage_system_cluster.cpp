#include "storage_system_cluster.h"

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>

#include <algorithm>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TStorageSystemCluster
    : public IStorage
{
private:
    const std::string TableName;
    IClusterNodeTrackerPtr ClusterNodeTracker;

public:
    TStorageSystemCluster(
        IClusterNodeTrackerPtr clusterNodeTracker,
        std::string tableName);

    std::string getName() const override
    {
        return "SystemCluster";
    }

    std::string getTableName() const override
    {
        return TableName;
    }

    BlockInputStreams read(
        const Names& columnNames,
        const SelectQueryInfo& queryInfo,
        const Context& context,
        QueryProcessingStage::Enum processedStage,
        size_t maxBlockSize,
        unsigned numStreams) override;

private:
    static NamesAndTypesList CreateColumnList();
};

////////////////////////////////////////////////////////////////////////////////

TStorageSystemCluster::TStorageSystemCluster(
    IClusterNodeTrackerPtr clusterNodeTracker,
    std::string tableName)
    : TableName(std::move(tableName))
    , ClusterNodeTracker(std::move(clusterNodeTracker))
{
    setColumns(ColumnsDescription(CreateColumnList()));
}

NamesAndTypesList TStorageSystemCluster::CreateColumnList()
{
    return {
        {
            "host",
            std::make_shared<DataTypeString>(),
        },
        {
            "port",
            std::make_shared<DataTypeUInt16>(),
        }
    };
}

BlockInputStreams TStorageSystemCluster::read(
    const Names& columnNames,
    const SelectQueryInfo& queryInfo,
    const Context& context,
    QueryProcessingStage::Enum processedStage,
    size_t maxBlockSize,
    unsigned numStreams)
{
    auto clusterNodes = ClusterNodeTracker->ListAvailableNodes();

    std::vector<TClusterNodeName> clusterNodeList(clusterNodes.begin(), clusterNodes.end());
    std::sort(clusterNodeList.begin(), clusterNodeList.end());

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    for (const auto& node : clusterNodeList) {
        res_columns[0]->insert(node.Host);
        res_columns[1]->insert(UInt64(node.Port));
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSystemCluster(
    IClusterNodeTrackerPtr clusterNodeTracker,
    std::string tableName)
{
    return std::make_shared<TStorageSystemCluster>(
        std::move(clusterNodeTracker),
        std::move(tableName));
}

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
