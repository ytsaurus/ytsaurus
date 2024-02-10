#include "cluster_nodes.h"
#include "storage_system_clique.h"

#include <yt/yt/library/clickhouse_discovery/discovery.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/IStorage.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <QueryPipeline/Pipe.h>

#include <algorithm>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TStorageSystemClique
    : public DB::IStorage
{
private:
    IDiscoveryPtr Discovery_;
    TGuid InstanceId_;

public:
    TStorageSystemClique(
        IDiscoveryPtr discovery,
        TGuid instanceId)
        : DB::IStorage({"system", "clique"})
        , Discovery_(std::move(discovery))
        , InstanceId_(std::move(instanceId))
    {
        DB::StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(CreateColumnList());
        setInMemoryMetadata(storage_metadata);
    }

    std::string getName() const override
    {
        return "SystemClique";
    }

    DB::Pipe read(
        const DB::Names& /*columnNames*/,
        const DB::StorageSnapshotPtr& storageSnapshot,
        DB::SelectQueryInfo& /*queryInfo*/,
        DB::ContextPtr /*context*/,
        DB::QueryProcessingStage::Enum /*processedStage*/,
        size_t /*maxBlockSize*/,
        size_t /*numStreams*/) override
    {
        auto nodes = Discovery_->List();

        auto metadataSnapshot = storageSnapshot->getMetadataForQuery();

        DB::MutableColumns res_columns = metadataSnapshot->getSampleBlock().cloneEmptyColumns();

        for (const auto& [name, attributes] : nodes) {
            if (!attributes || !attributes->Contains("clique_id")) {
                continue;
            }
            res_columns[0]->insert(std::string(attributes->Get<TString>("host")));
            res_columns[1]->insert(attributes->Get<ui64>("rpc_port"));
            res_columns[2]->insert(attributes->Get<ui64>("monitoring_port"));
            res_columns[3]->insert(attributes->Get<ui64>("tcp_port"));
            res_columns[4]->insert(attributes->Get<ui64>("http_port"));
            res_columns[5]->insert(std::string(name));
            res_columns[6]->insert(attributes->Get<i64>("pid"));
            res_columns[7]->insert(name == ToString(InstanceId_));
            res_columns[8]->insert(attributes->Get<ui64>("job_cookie"));
            res_columns[9]->insert(static_cast<DB::Decimal64>(attributes->Get<TInstant>("start_time").MicroSeconds()));
            res_columns[10]->insert(std::string(attributes->Get<TString>("clique_id")));
            res_columns[11]->insert(attributes->Get<i64>("clique_incarnation"));
        }

        auto blockInputStream = std::make_shared<DB::OneBlockInputStream>(metadataSnapshot->getSampleBlock().cloneWithColumns(std::move(res_columns)));
        auto source = std::make_shared<DB::SourceFromInputStream>(std::move(blockInputStream));
        return DB::Pipe(std::move(source));
    }

private:
    static DB::ColumnsDescription CreateColumnList()
    {
        return DB::ColumnsDescription({
            {"host", std::make_shared<DB::DataTypeString>()},
            {"rpc_port", std::make_shared<DB::DataTypeUInt16>()},
            {"monitoring_port", std::make_shared<DB::DataTypeUInt16>()},
            {"tcp_port", std::make_shared<DB::DataTypeUInt16>()},
            {"http_port", std::make_shared<DB::DataTypeUInt16>()},
            {"job_id", std::make_shared<DB::DataTypeString>()},
            {"pid", std::make_shared<DB::DataTypeInt32>()},
            {"self", std::make_shared<DB::DataTypeUInt8>()},
            {"job_cookie", std::make_shared<DB::DataTypeUInt32>()},
            {"start_time", std::make_shared<DB::DataTypeDateTime64>(6)},
            {"clique_id", std::make_shared<DB::DataTypeString>()},
            {"clique_incarnation", std::make_shared<DB::DataTypeInt64>()},
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSystemClique(
    IDiscoveryPtr discovery,
    TGuid instanceId)
{
    return std::make_shared<TStorageSystemClique>(
        std::move(discovery),
        instanceId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
