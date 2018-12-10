#include "artifact.h"

#include <yt/core/misc/hash.h>
#include <yt/core/misc/protobuf_helpers.h>

#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TArtifactKey::TArtifactKey(const TChunkId& chunkId)
{
    mutable_data_source()->set_type(static_cast<int>(EDataSourceType::File));
    NChunkClient::NProto::TChunkSpec chunkSpec;
    ToProto(chunkSpec.mutable_chunk_id(), chunkId);
    *add_chunk_specs() = chunkSpec;
}

TArtifactKey::TArtifactKey(const NScheduler::NProto::TFileDescriptor& descriptor)
{
    mutable_data_source()->MergeFrom(descriptor.data_source());

    if (descriptor.chunk_specs_size() > 0) {
        mutable_chunk_specs()->MergeFrom(descriptor.chunk_specs());
    }

    if (descriptor.has_format()) {
        set_format(descriptor.format());
    }
}

TArtifactKey::operator size_t() const
{
    size_t result = 0;
    HashCombine(result, data_source().type());

    if (has_format()) {
        HashCombine(result, format());
    }

    if (data_source().has_column_filter()) {
        for (const auto& column : data_source().columns()) {
            HashCombine(result, column);
        }
    }

    if (data_source().has_timestamp()) {
        HashCombine(result, data_source().timestamp());
    }

    YCHECK(!data_source().has_table_schema_id());

    if (data_source().has_table_schema()) {
        for (const auto& column : data_source().table_schema().columns()) {
            HashCombine(result, column.name());
            HashCombine(result, column.type());
        }
    }

    auto hashReadLimit = [&] (const NChunkClient::NProto::TReadLimit& limit) {
        if (limit.has_row_index()) {
            HashCombine(result, limit.row_index());
        }
        if (limit.has_offset()) {
            HashCombine(result, limit.offset());
        }
        if (limit.has_key()) {
            HashCombine(result, limit.key());
        }
    };

    for (const auto& spec : chunk_specs()) {
        auto id = FromProto<TGuid>(spec.chunk_id());
        HashCombine(result, id);

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
    if (data_source().type() != other.data_source().type())
        return false;

    if (has_format() != other.has_format())
        return false;

    if (has_format() && format() != other.format())
        return false;

    if (data_source().has_table_schema() != other.data_source().has_table_schema())
        return false;

    if (data_source().has_table_schema()) {
        auto lhsSchema = FromProto<TTableSchema>(data_source().table_schema());
        auto rhsSchema = FromProto<TTableSchema>(other.data_source().table_schema());

        if (lhsSchema != rhsSchema) {
            return false;
        }
    }

    if (data_source().has_timestamp() != other.data_source().has_timestamp())
        return false;

    if (data_source().has_timestamp() && data_source().timestamp() != other.data_source().timestamp())
        return false;

    if (data_source().has_column_filter() != other.data_source().has_column_filter())
        return false;

    if (data_source().has_column_filter()) {
        auto lhsColumns = FromProto<std::vector<TString>>(data_source().columns());
        auto rhsColumns = FromProto<std::vector<TString>>(other.data_source().columns());
        if (lhsColumns != rhsColumns) {
            return false;
        }
    }

    if (chunk_specs_size() != other.chunk_specs_size())
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

    for (int index = 0; index < chunk_specs_size(); ++index) {
        const auto& lhs = chunk_specs(index);
        const auto& rhs = other.chunk_specs(index);

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

TString ToString(const TArtifactKey& key)
{
    return Format("{%v}", key.DebugString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespaca NYT::NDataNode
