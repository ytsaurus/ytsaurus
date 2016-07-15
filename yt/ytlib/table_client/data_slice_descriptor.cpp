#include  "data_slice_descriptor.h"

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/helpers.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TDataSliceDescriptor::TDataSliceDescriptor(
    EDataSliceDescriptorType type,
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs)
    : Type(type)
    , ChunkSpecs(std::move(chunkSpecs))
{ }

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TDataSliceDescriptor* protoDataSliceDescriptor, const TDataSliceDescriptor& dataSliceDescriptor)
{
    protoDataSliceDescriptor->set_type(static_cast<int>(dataSliceDescriptor.Type));
    for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
        auto* protoChunkSpec = protoDataSliceDescriptor->add_chunks();
        *protoChunkSpec = chunkSpec;
    }
    if (dataSliceDescriptor.Type == EDataSliceDescriptorType::UnversionedTable ||
        dataSliceDescriptor.Type == EDataSliceDescriptorType::VersionedTable)
    {
        auto* tableSliceDescriptor = protoDataSliceDescriptor->MutableExtension(NProto::TTableSliceDescriptor::table_slice_descriptor);
        ToProto(tableSliceDescriptor->mutable_schema(), dataSliceDescriptor.Schema);
        tableSliceDescriptor->set_timestamp(static_cast<i64>(dataSliceDescriptor.Timestamp));
    }
}

void FromProto(TDataSliceDescriptor* dataSliceDescriptor, const NProto::TDataSliceDescriptor& protoDataSliceDescriptor)
{
    dataSliceDescriptor->Type = EDataSliceDescriptorType(protoDataSliceDescriptor.type());
    dataSliceDescriptor->ChunkSpecs = std::vector<TChunkSpec>(protoDataSliceDescriptor.chunks().begin(), protoDataSliceDescriptor.chunks().end());
    if (dataSliceDescriptor->Type == EDataSliceDescriptorType::UnversionedTable ||
        dataSliceDescriptor->Type == EDataSliceDescriptorType::VersionedTable)
    {
        auto tableSliceDescriptor = protoDataSliceDescriptor.GetExtension(NProto::TTableSliceDescriptor::table_slice_descriptor);
        FromProto(&dataSliceDescriptor->Schema, tableSliceDescriptor.schema());
        dataSliceDescriptor->Timestamp = TTimestamp(tableSliceDescriptor.timestamp());
    }
}

////////////////////////////////////////////////////////////////////////////////

i64 GetCumulativeRowCount(const std::vector<TDataSliceDescriptor>& dataSliceDescriptors)
{
    i64 result = 0;
    for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
        result += NChunkClient::GetCumulativeRowCount(dataSliceDescriptor.ChunkSpecs);
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

} // namespace NChunkClient
} // namespace NYT
