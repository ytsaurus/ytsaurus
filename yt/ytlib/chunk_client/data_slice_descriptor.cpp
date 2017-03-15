#include "data_slice_descriptor.h"
#include "chunk_spec.h"
#include "helpers.h"

namespace NYT {
namespace NChunkClient {

using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TDataSliceDescriptor::TDataSliceDescriptor(std::vector<NProto::TChunkSpec> chunkSpecs)
    : ChunkSpecs(std::move(chunkSpecs))
{ }

TDataSliceDescriptor::TDataSliceDescriptor(const NProto::TChunkSpec& chunkSpec)
{
    ChunkSpecs.push_back(chunkSpec);
}

const NProto::TChunkSpec& TDataSliceDescriptor::GetSingleChunk() const
{
    YCHECK(ChunkSpecs.size() == 1);
    return ChunkSpecs[0];
}

TNullable<i64> TDataSliceDescriptor::GetCommonTag() const
{
    YCHECK(!ChunkSpecs.empty());
    TNullable<i64> commonTag = ChunkSpecs.front().has_data_slice_tag()
        ? MakeNullable(ChunkSpecs.front().data_slice_tag())
        : Null;
    for (const auto& chunkSpec : ChunkSpecs) {
        TNullable<i64> tag = chunkSpec.has_data_slice_tag()
            ? MakeNullable(chunkSpec.data_slice_tag())
            : Null;
        YCHECK(commonTag == tag);
    }
    return commonTag;
}

int TDataSliceDescriptor::GetDataSourceIndex() const
{
    return ChunkSpecs.empty()
        ? 0
        : ChunkSpecs.front().table_index();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TDataSliceDescriptor* protoDataSliceDescriptor, const TDataSliceDescriptor& dataSliceDescriptor)
{
    for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
        *protoDataSliceDescriptor->add_chunks() = chunkSpec;
    }
}

void FromProto(TDataSliceDescriptor* dataSliceDescriptor, const NProto::TDataSliceDescriptor& protoDataSliceDescriptor)
{
    dataSliceDescriptor->ChunkSpecs = std::vector<NProto::TChunkSpec>(protoDataSliceDescriptor.chunks().begin(), protoDataSliceDescriptor.chunks().end());
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

} // namespace NChunkClient
} // namespace NYT
