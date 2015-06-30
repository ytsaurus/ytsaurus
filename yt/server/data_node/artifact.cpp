#include "artifact.h"

#include <core/misc/hash.h>
#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TArtifactKey::TArtifactKey(const TChunkId& chunkId)
{
    set_type(static_cast<int>(EObjectType::File));
    ToProto(add_chunks()->mutable_chunk_id(), chunkId);
}

TArtifactKey::TArtifactKey(const NScheduler::NProto::TFileDescriptor& descriptor)
{
    set_type(descriptor.type());
    mutable_chunks()->MergeFrom(descriptor.chunks());
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

    for (const auto& spec : chunks()) {
        auto id = FromProto<TGuid>(spec.chunk_id());
        result = HashCombine(result, id);

        if (spec.has_lower_limit()) {
            hashReadLimit(spec.lower_limit());
        }
        if (spec.has_upper_limit()) {
            hashReadLimit(spec.upper_limit());
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

    if (chunks_size() != other.chunks_size())
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

    for (int index = 0; index < chunks_size(); ++index) {
        const auto& lhs = chunks(index);
        const auto& rhs = other.chunks(index);

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

    return true;
}

Stroka ToString(const TArtifactKey& key)
{
    return key.DebugString();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespaca NYT

