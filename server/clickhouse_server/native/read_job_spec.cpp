#include "read_job_spec.h"

#include "serialize.h"
#include "table_schema.h"

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NYson;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsTable(const TDataSource& dataSource)
{
    auto type = dataSource.GetType();

    return type == EDataSourceType::UnversionedTable ||
           type == EDataSourceType::VersionedTable;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TReadJobSpec::Validate() const
{
    const auto& dataSources = DataSources();

    if (dataSources.empty()) {
        THROW_ERROR_EXCEPTION("Invalid job specification: empty data sources list");
    }

    for (auto& dataSource : dataSources) {
        if (!dataSource.GetPath()) {
            THROW_ERROR_EXCEPTION("Invalid job specification: table path not found");
        }
        if (!dataSource.Schema()) {
            THROW_ERROR_EXCEPTION("Invalid job specification: table schema not found");
        }
        if (!IsTable(dataSource)) {
            THROW_ERROR_EXCEPTION(
                "Invalid job specification: unsupported data source type %Qlv",
                dataSource.GetType());
        }
    }

    const auto& representativeDataSource = dataSources.front();

    for (size_t i = 1; i < dataSources.size(); ++i) {
        auto dataSource = dataSources[i];

        if (*dataSource.Schema() != representativeDataSource.Schema()) {
            THROW_ERROR_EXCEPTION("Invalid job specification: inconsistent schemas");
        }
        if (dataSource.GetType() != representativeDataSource.GetType()) {
            THROW_ERROR_EXCEPTION("Invalid job specification: inconsistent data source types");
        }
    }

    if (DataSliceDescriptors.empty()) {
        THROW_ERROR_EXCEPTION("Invalid job specification: empty data slice desciptors list");
    }
}

NChunkClient::EDataSourceType TReadJobSpec::GetCommonDataSourceType() const
{
    // TODO: checks
    const auto& representative = DataSources().front();
    return representative.GetType();
}

NTableClient::TTableSchema TReadJobSpec::GetCommonNativeSchema() const
{
    // TODO: checks
    const auto& representative = DataSources().front();
    return *representative.Schema();
}

TTableList TReadJobSpec::GetTables() const
{
    auto nativeSchema = GetCommonNativeSchema();

    const auto& dataSources = DataSources();

    TTableList tables;
    tables.reserve(dataSources.size());
    for (auto dataSource : dataSources) {
        tables.push_back(
            CreateTableSchema(*dataSource.GetPath(), nativeSchema));
    }
    return tables;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TReadJobSpec* protoSpec, const TReadJobSpec& spec)
{
    ToProto(protoSpec->mutable_data_source_directory(), spec.DataSourceDirectory);

    auto* tableSpec = protoSpec->mutable_table_spec();
    ToProto(
        tableSpec->mutable_chunk_specs(),
        tableSpec->mutable_chunk_spec_count_per_data_slice(),
        spec.DataSliceDescriptors);

    if (spec.NodeDirectory) {
        spec.NodeDirectory->DumpTo(protoSpec->mutable_node_directory());
    }
}

void FromProto(TReadJobSpec* spec, const NProto::TReadJobSpec& protoSpec)
{
    FromProto(&spec->DataSourceDirectory, protoSpec.data_source_directory());

    const auto& tableSpec = protoSpec.table_spec();
    FromProto(
        &spec->DataSliceDescriptors,
        tableSpec.chunk_specs(),
        tableSpec.chunk_spec_count_per_data_slice());

    if (protoSpec.has_node_directory()) {
        spec->NodeDirectory = New<TNodeDirectory>();
        spec->NodeDirectory->MergeFrom(protoSpec.node_directory());
    }
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TReadJobSpec& spec, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("data_source_directory").Value(spec.DataSourceDirectory)
            .Item("table_spec").List(spec.DataSliceDescriptors)
            .DoIf(bool(spec.NodeDirectory), [&] (TFluentMap fluent) {
                fluent.Item("node_directory").Value(spec.NodeDirectory);
            })
        .EndMap();
}

void Deserialize(TReadJobSpec& spec, INodePtr node)
{
    auto mapNode = node->AsMap();

    spec = TReadJobSpec();
    spec.DataSourceDirectory = ConvertTo<TDataSourceDirectoryPtr>(mapNode->GetChild("data_source_directory"));
    spec.DataSliceDescriptors = ConvertTo<std::vector<TDataSliceDescriptor>>(mapNode->GetChild("table_spec"));

    if (auto node = mapNode->FindChild("node_directory")) {
        spec.NodeDirectory = ConvertTo<TNodeDirectoryPtr>(node);
    }
}

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
