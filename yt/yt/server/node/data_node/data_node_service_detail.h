#pragma once

#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/client/table_client/key_bound.h>

#include <variant>

namespace NYT::NDataNode::NDetail {

////////////////////////////////////////////////////////////////////////////////

using TTableSampleKeyBound = std::variant<
    NTableClient::TOwningKeyBound,
    NTableClient::TLegacyOwningKey
>;

struct TTableSampleKeyRange
{
    TTableSampleKeyBound LowerBound;
    TTableSampleKeyBound UpperBound;
};

TTableSampleKeyRange GetTableSampleKeyRange(
    const NChunkClient::NProto::TReqGetTableSamples::TSampleRequest& request,
    int keyLength);

bool IsTableSampleKeyInRange(
    NTableClient::TUnversionedRow key,
    const TTableSampleKeyRange& range);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode::NDetail
