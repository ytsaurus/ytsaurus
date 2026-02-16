#include "subquery_spec.h"

#include "config.h"
#include "conversion.h"

#include <yt/yt/client/table_client/config.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <DataTypes/DataTypeFactory.h>

namespace NYT::NClickHouseServer {

using namespace NChunkClient;
using namespace NChunkPools;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void FillDataSliceDescriptors(
    TSecondaryQueryReadDescriptors& dataSliceDescriptors,
    const THashMap<TChunkId, TRefCountedMiscExtPtr>& miscExtMap,
    const TChunkStripePtr& chunkStripe)
{
    for (const auto& dataSlice : chunkStripe->DataSlices()) {
        auto& inputDataSliceDescriptor = dataSliceDescriptors.emplace_back();
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            auto& chunkSpec = inputDataSliceDescriptor.ChunkSpecs.emplace_back();
            ToProto(&chunkSpec, chunkSlice, /*comparator*/ TComparator(), dataSlice->Type);
            auto it = GetIteratorOrCrash(miscExtMap, chunkSlice->GetInputChunk()->GetChunkId());
            if (it->second) {
                SetProtoExtension(
                    chunkSpec.mutable_chunk_meta()->mutable_extensions(),
                    static_cast<const NChunkClient::NProto::TMiscExt&>(*it->second));
            }
        }
        inputDataSliceDescriptor.VirtualRowIndex = dataSlice->VirtualRowIndex;
    }
}

void FillDataSliceDescriptors(
    std::vector<TSecondaryQueryReadDescriptors>& dataSliceDescriptors,
    const THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr>& miscExtMap,
    const TRange<NChunkPools::TChunkStripePtr>& chunkStripes)
{
    for (const auto& chunkStripe : chunkStripes) {
        auto& inputDataSliceDescriptors = dataSliceDescriptors.emplace_back();
        FillDataSliceDescriptors(inputDataSliceDescriptors, miscExtMap, chunkStripe);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSubquerySpec* protoSpec, const TSubquerySpec& spec)
{
    using NYT::ToProto;

    ToProto(protoSpec->mutable_data_source_directory(), spec.DataSourceDirectory);

    protoSpec->set_input_stream_count(spec.InputStreamCount);
    protoSpec->set_input_specs_truncated(spec.InputSpecsTruncated);
    for (const auto& inputDataSliceDescriptors : spec.InputSpecs) {
        auto* inputSpec = protoSpec->add_input_specs();
        ToProto(
            inputSpec->mutable_chunk_specs(),
            inputSpec->mutable_chunk_spec_count_per_data_slice(),
            inputSpec->mutable_virtual_row_index_per_data_slice(),
            inputDataSliceDescriptors);
    }

    ToProto(protoSpec->mutable_read_schema(), spec.ReadSchema);

    protoSpec->set_subquery_index(spec.SubqueryIndex);
    protoSpec->set_table_index(spec.TableIndex);
    protoSpec->set_initial_query(spec.InitialQuery);
    protoSpec->set_table_reader_config(ConvertToYsonString(spec.TableReaderConfig).ToString());
    protoSpec->set_query_settings(ConvertToYsonString(spec.QuerySettings).ToString());
    if (spec.TableStatistics.has_value()) {
        ToProto(protoSpec->mutable_table_stats(), *spec.TableStatistics);
    }
    for (const auto& attributes : spec.ColumnAttributes) {
        NYTree::ToProto(protoSpec->add_column_attributes(), *attributes);
    }
}

void FromProto(TSubquerySpec* spec, const NProto::TSubquerySpec& protoSpec)
{
    using NYT::FromProto;

    FromProto(&spec->DataSourceDirectory, protoSpec.data_source_directory());

    spec->InputStreamCount = protoSpec.input_stream_count();
    spec->InputSpecsTruncated = protoSpec.input_specs_truncated();
    for (const auto& inputSpec : protoSpec.input_specs()) {
        FromProto(
            &spec->InputSpecs.emplace_back(),
            inputSpec.chunk_specs(),
            inputSpec.chunk_spec_count_per_data_slice(),
            inputSpec.virtual_row_index_per_data_slice());
    }

    FromProto(&spec->ReadSchema, protoSpec.read_schema());

    spec->SubqueryIndex = protoSpec.subquery_index();
    spec->TableIndex = protoSpec.table_index();
    spec->InitialQuery = protoSpec.initial_query();

    auto tableReaderConfigYson = TYsonString(protoSpec.table_reader_config());
    spec->TableReaderConfig = ConvertTo<TTableReaderConfigPtr>(tableReaderConfigYson);
    auto querySettingsYson = TYsonString(protoSpec.query_settings());
    spec->QuerySettings = ConvertTo<TQuerySettingsPtr>(querySettingsYson);
    if (protoSpec.has_table_stats()) {
        spec->TableStatistics.emplace();
        FromProto(&spec->TableStatistics.value(), protoSpec.table_stats(), nullptr, protoSpec.table_stats().chunk_row_count());
    }
    spec->ColumnAttributes.reserve(protoSpec.column_attributes_size());
    for (const auto& attributes : protoSpec.column_attributes()) {
        spec->ColumnAttributes.emplace_back(NYTree::FromProto(attributes));
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSecondaryQueryReadTask* protoTask, const TSecondaryQueryReadTask& task)
{
    using NYT::ToProto;

    for (const auto& operandInput : task.OperandInputs) {
        auto* protoOperandInput = protoTask->add_operand_inputs();
        ToProto(
            protoOperandInput->mutable_chunk_specs(),
            protoOperandInput->mutable_chunk_spec_count_per_data_slice(),
            protoOperandInput->mutable_virtual_row_index_per_data_slice(),
            operandInput);
    }
}

void FromProto(TSecondaryQueryReadTask* task, const NProto::TSecondaryQueryReadTask& protoTask)
{
    using NYT::FromProto;

    for (const auto& protoOperandInput : protoTask.operand_inputs()) {
        auto& operandInput = task->OperandInputs.emplace_back();
        FromProto(
            &operandInput,
            protoOperandInput.chunk_specs(),
            protoOperandInput.chunk_spec_count_per_data_slice(),
            protoOperandInput.virtual_row_index_per_data_slice());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
