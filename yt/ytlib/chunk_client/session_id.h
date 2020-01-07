#pragma once

#include "public.h"

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/ytlib/chunk_client/proto/session_id.pb.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TSessionId
{
    TSessionId();
    TSessionId(TChunkId chunkId, int mediumIndex);

    TChunkId ChunkId;
    int MediumIndex;
};

bool operator==(TSessionId lhs, TSessionId rhs);
bool operator!=(TSessionId lhs, TSessionId rhs);

void FormatValue(TStringBuilderBase* builder, TSessionId id, TStringBuf format);
TString ToString(TSessionId id);

void ToProto(NProto::TSessionId* protoSessionId, TSessionId sessionId);
void FromProto(TSessionId* sessionId, const NProto::TSessionId& protoSessionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

Y_DECLARE_PODTYPE(NYT::NChunkClient::TSessionId);

//! A hasher for TSessionId.
template <>
struct THash<NYT::NChunkClient::TSessionId>
{
    inline size_t operator()(NYT::NChunkClient::TSessionId value) const
    {
        return THash<NYT::NChunkClient::TChunkId>()(value.ChunkId) * 497 + value.MediumIndex;
    }
};

#define SESSION_ID_INL_H_
#include "session_id-inl.h"
#undef SESSION_ID_INL_H_
