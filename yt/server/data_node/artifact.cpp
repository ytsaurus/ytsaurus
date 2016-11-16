#include "artifact.h"

#include <yt/core/misc/hash.h>
#include <yt/core/misc/protobuf_helpers.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TArtifactKey::TArtifactKey(const TChunkId& chunkId)
{
    set_type(static_cast<int>(EObjectType::File));
    NChunkClient::NProto::TChunkSpec chunkSpec;
    ToProto(chunkSpec.mutable_chunk_id(), chunkId);
    ToProto(add_data_slice_descriptors(), MakeFileDataSliceDescriptor(chunkSpec));
}

TArtifactKey::TArtifactKey(const NScheduler::NProto::TFileDescriptor& descriptor)
{
    set_type(descriptor.type());
    mutable_data_slice_descriptors()->MergeFrom(descriptor.data_slice_descriptors());
    if (descriptor.has_format()) {
        set_format(descriptor.format());
    }
}

TArtifactKey::operator size_t() const
{
    size_t result = 0;
    result = HashCombine(result, type());

    if (has_format()) {
        result = HashCombine(result, format());
    }

    auto hashReadLimit = [&] (const NChunkClient::NProto::TReadLimit& limit) {
        if (limit.has_row_index()) {
            result = HashCombine(result, limit.row_index());
        }
        if (limit.has_offset()) {
            result = HashCombine(result, limit.offset());
        }
        if (limit.has_key()) {
            result = HashCombine(result, limit.key());
        }
    };

    auto dataSliceDescriptors = FromProto<std::vector<TDataSliceDescriptor>>(data_slice_descriptors());
    for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
        result = HashCombine(result, dataSliceDescriptor.Type);

        for (const auto& spec : dataSliceDescriptor.ChunkSpecs) {
            auto id = FromProto<TGuid>(spec.chunk_id());
            result = HashCombine(result, id);

            if (spec.has_lower_limit()) {
                hashReadLimit(spec.lower_limit());
            }
            if (spec.has_upper_limit()) {
                hashReadLimit(spec.upper_limit());
            }
        }
    }

    return result;
}

bool TArtifactKey::operator == (const TArtifactKey& other) const
{
    if (type() != other.type())
        return false;

    if (has_format() != other.has_format())
        return false;

    if (has_format() && format() != other.format())
        return false;

    if (data_slice_descriptors_size() != other.data_slice_descriptors_size())
        return false;

    auto compareLimits = [] (
        const NChunkClient::NProto::TReadLimit& lhs,
        const NChunkClient::NProto::TReadLimit& rhs)
    {
        if (lhs.has_row_index() != rhs.has_row_index())
            return false;

        if (lhs.has_row_index() && lhs.row_index() != rhs.row_index())
            return false;

        if (lhs.has_offset() != rhs.has_offset())
            return false;

        if (lhs.has_offset() && lhs.offset() != rhs.offset())
            return false;

        if (lhs.has_key() != rhs.has_key())
            return false;

        if (lhs.has_key() && lhs.key() != rhs.key())
            return false;

        return true;
    };

    auto dataSliceDescriptors = FromProto<std::vector<TDataSliceDescriptor>>(data_slice_descriptors());
    auto otherDataSliceDescriptors = FromProto<std::vector<TDataSliceDescriptor>>(other.data_slice_descriptors());

    for (int index = 0; index < dataSliceDescriptors.size(); ++index) {
        const auto& descriptor = dataSliceDescriptors[index];
        const auto& otherDescriptor = otherDataSliceDescriptors[index];

        if (descriptor.Type != otherDescriptor.Type)
            return false;

        if (descriptor.Schema != otherDescriptor.Schema)
            return false;

        if (descriptor.Timestamp != otherDescriptor.Timestamp)
            return false;

        if (descriptor.ChunkSpecs.size() != otherDescriptor.ChunkSpecs.size())
            return false;

        for (int chunkIndex = 0; chunkIndex < descriptor.ChunkSpecs.size(); ++chunkIndex) {
            const auto& lhs = descriptor.ChunkSpecs[chunkIndex];
            const auto& rhs = otherDescriptor.ChunkSpecs[chunkIndex];

            auto leftId = FromProto<TGuid>(lhs.chunk_id());
            auto rightId = FromProto<TGuid>(rhs.chunk_id());

            if (leftId != rightId)
                return false;

            if (lhs.has_lower_limit() != rhs.has_lower_limit())
                return false;

            if (lhs.has_lower_limit() && !compareLimits(lhs.lower_limit(), rhs.lower_limit()))
                return false;

            if (lhs.has_upper_limit() != rhs.has_upper_limit())
                return false;

            if (lhs.has_upper_limit() && !compareLimits(lhs.upper_limit(), rhs.upper_limit()))
                return false;
        }
    }

    return true;
}

Stroka ToString(const TArtifactKey& key)
{
    return Format("{%v}", key.DebugString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespaca NYT

