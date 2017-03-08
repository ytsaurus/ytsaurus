#include "data_slice_descriptor.h"
#include "chunk_spec.h"
#include "helpers.h"

namespace NYT {
namespace NChunkClient {

using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TDataSliceDescriptor::TDataSliceDescriptor(
    EDataSliceDescriptorType type,
    std::vector<NProto::TChunkSpec> chunkSpecs,
    const TTableSchema& schema,
    TTimestamp timestamp)
    : Type(type)
    , ChunkSpecs(std::move(chunkSpecs))
    , Schema(schema)
    , Timestamp(timestamp)
{ }

const NProto::TChunkSpec& TDataSliceDescriptor::GetSingleUnversionedChunk() const
{
    YCHECK(Type == EDataSliceDescriptorType::UnversionedTable);
    YCHECK(ChunkSpecs.size() == 1);
    return ChunkSpecs[0];
}

const NProto::TChunkSpec& TDataSliceDescriptor::GetSingleFileChunk() const
{
    YCHECK(Type == EDataSliceDescriptorType::File);
    YCHECK(ChunkSpecs.size() == 1);
    return ChunkSpecs[0];
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TDataSliceDescriptor* protoDataSliceDescriptor, const TDataSliceDescriptor& dataSliceDescriptor)
{
    protoDataSliceDescriptor->set_type(static_cast<int>(dataSliceDescriptor.Type));
    for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
        *protoDataSliceDescriptor->add_chunks() = chunkSpec;
    }

    // Currently schema is required only for versioned slices.
    if (dataSliceDescriptor.Type == EDataSliceDescriptorType::VersionedTable) {
        ToProto(protoDataSliceDescriptor->mutable_schema(), dataSliceDescriptor.Schema);
        protoDataSliceDescriptor->set_timestamp(static_cast<i64>(dataSliceDescriptor.Timestamp));
    }
}

void FromProto(TDataSliceDescriptor* dataSliceDescriptor, const NProto::TDataSliceDescriptor& protoDataSliceDescriptor)
{
    dataSliceDescriptor->Type = EDataSliceDescriptorType(protoDataSliceDescriptor.type());
    dataSliceDescriptor->ChunkSpecs = std::vector<NProto::TChunkSpec>(protoDataSliceDescriptor.chunks().begin(), protoDataSliceDescriptor.chunks().end());
    if (dataSliceDescriptor->Type == EDataSliceDescriptorType::VersionedTable) {
        if (protoDataSliceDescriptor.HasExtension(NProto::TTableSliceDescriptor::table_slice_descriptor)) {
            // COMPAT(psushin)
            auto tableSliceDescriptor = protoDataSliceDescriptor.GetExtension(NProto::TTableSliceDescriptor::table_slice_descriptor);
            FromProto(&dataSliceDescriptor->Schema, tableSliceDescriptor.schema());
            dataSliceDescriptor->Timestamp = NTableClient::TTimestamp(tableSliceDescriptor.timestamp());
        } else {
            FromProto(&dataSliceDescriptor->Schema, protoDataSliceDescriptor.schema());
            dataSliceDescriptor->Timestamp = NTableClient::TTimestamp(protoDataSliceDescriptor.timestamp());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

i64 GetCumulativeRowCount(const std::vector<TDataSliceDescriptor>& dataSliceDescriptors)
{
    i64 result = 0;
    for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
        result += GetCumulativeRowCount(dataSliceDescriptor.ChunkSpecs);
    }
    return result;
}

i64 GetDataSliceDescriptorReaderMemoryEstimate(const TDataSliceDescriptor& dataSliceDescriptor, TMultiChunkReaderConfigPtr config)
{
    i64 result = 0;
    for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
        result += GetChunkReaderMemoryEstimate(chunkSpec, config);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TDataSliceDescriptor MakeFileDataSliceDescriptor(NProto::TChunkSpec chunkSpec)
{
    return TDataSliceDescriptor(EDataSliceDescriptorType::File, {std::move(chunkSpec)});
}

TDataSliceDescriptor MakeUnversionedDataSliceDescriptor(NProto::TChunkSpec chunkSpec)
{
    return TDataSliceDescriptor(EDataSliceDescriptorType::UnversionedTable, {std::move(chunkSpec)});
}

TDataSliceDescriptor MakeVersionedDataSliceDescriptor(
    std::vector<NProto::TChunkSpec> chunkSpecs,
    const NTableClient::TTableSchema& schema,
    NTransactionClient::TTimestamp timestamp)
{
    return TDataSliceDescriptor(
        EDataSliceDescriptorType::VersionedTable,
        std::move(chunkSpecs),
        schema,
        timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
