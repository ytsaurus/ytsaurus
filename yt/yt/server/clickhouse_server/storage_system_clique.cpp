#include "cluster_nodes.h"
#include "storage_system_clique.h"

#include <yt/client/misc/discovery.h>

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

    DB::Pipes read(
        const DB::Names& /* columnNames */,
        const DB::StorageMetadataPtr& metadata_snapshot,
        const DB::SelectQueryInfo& /* queryInfo */,
        const DB::Context& /* context */,
        DB::QueryProcessingStage::Enum /* processedStage */,
        size_t /* maxBlockSize */,
        unsigned /* numStreams */)
    {
        auto nodes = Discovery_->List();

        DB::MutableColumns res_columns = metadata_snapshot->getSampleBlock().cloneEmptyColumns();

        for (const auto& [name, attributes] : nodes) {
            res_columns[0]->insert(std::string(attributes.at("host")->GetValue<TString>()));
            res_columns[1]->insert(attributes.at("rpc_port")->GetValue<ui64>());
            res_columns[2]->insert(attributes.at("monitoring_port")->GetValue<ui64>());
            res_columns[3]->insert(attributes.at("tcp_port")->GetValue<ui64>());
            res_columns[4]->insert(attributes.at("http_port")->GetValue<ui64>());
            res_columns[5]->insert(std::string(name));
            res_columns[6]->insert(attributes.at("pid")->GetValue<i64>());
            res_columns[7]->insert(name == ToString(InstanceId_));
            res_columns[8]->insert(attributes.at("job_cookie")->GetValue<ui64>());
        }

        auto blockInputStream = std::make_shared<DB::OneBlockInputStream>(metadata_snapshot->getSampleBlock().cloneWithColumns(std::move(res_columns)));
        auto source = std::make_shared<DB::SourceFromInputStream>(std::move(blockInputStream));
        DB::Pipe pipe(std::move(source));
        DB::Pipes result;
        result.emplace_back(std::move(pipe));
        return result;
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
