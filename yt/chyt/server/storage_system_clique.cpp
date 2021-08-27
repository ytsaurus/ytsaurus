#include "cluster_nodes.h"
#include "storage_system_clique.h"

#include <yt/yt/client/misc/discovery.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/IStorage.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>

#include <algorithm>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TStorageSystemClique
    : public DB::IStorage
{
private:
    TDiscoveryPtr Discovery_;
    TGuid InstanceId_;

public:
    TStorageSystemClique(
        TDiscoveryPtr discovery,
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
        const DB::Names& /* columnNames */,
        const DB::StorageMetadataPtr& metadata_snapshot,
        DB::SelectQueryInfo& /* queryInfo */,
        DB::ContextPtr /* context */,
        DB::QueryProcessingStage::Enum /* processedStage */,
        size_t /* maxBlockSize */,
        unsigned /* numStreams */) override
    {
        auto nodes = Discovery_->List();

        DB::MutableColumns res_columns = metadata_snapshot->getSampleBlock().cloneEmptyColumns();

        for (const auto& [name, attributes] : nodes) {
            res_columns[0]->insert(std::string(attributes->Get<TString>("host")));
            res_columns[1]->insert(attributes->Get<ui64>("rpc_port"));
            res_columns[2]->insert(attributes->Get<ui64>("monitoring_port"));
            res_columns[3]->insert(attributes->Get<ui64>("tcp_port"));
            res_columns[4]->insert(attributes->Get<ui64>("http_port"));
            res_columns[5]->insert(std::string(name));
            res_columns[6]->insert(attributes->Get<i64>("pid"));
            res_columns[7]->insert(name == ToString(InstanceId_));
            res_columns[8]->insert(attributes->Get<ui64>("job_cookie"));
        }

        auto blockInputStream = std::make_shared<DB::OneBlockInputStream>(metadata_snapshot->getSampleBlock().cloneWithColumns(std::move(res_columns)));
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
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageSystemClique(
    TDiscoveryPtr discovery,
    TGuid instanceId)
{
    return std::make_shared<TStorageSystemClique>(
        std::move(discovery),
        instanceId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
