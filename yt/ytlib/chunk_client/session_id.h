#pragma once

#include "public.h"

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/ytlib/chunk_client/session_id.pb.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TSessionId
{
    TSessionId();
    TSessionId(const TChunkId& chunkId, int mediumIndex);

    TChunkId ChunkId;
    int MediumIndex;
};

bool operator==(const TSessionId& lhs, const TSessionId& rhs);
bool operator!=(const TSessionId& lhs, const TSessionId& rhs);

void FormatValue(TStringBuilder* builder, const TSessionId& id, TStringBuf format);
TString ToString(const TSessionId& id);

void ToProto(NProto::TSessionId* protoSessionId, const TSessionId& sessionId);
void FromProto(TSessionId* sessionId, const NProto::TSessionId& protoSessionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

Y_DECLARE_PODTYPE(NYT::NChunkClient::TSessionId);

//! A hasher for TSessionId.
template <>
struct THash<NYT::NChunkClient::TSessionId>
{
    inline size_t operator()(const NYT::NChunkClient::TSessionId& value) const
    {
        return THash<NYT::NChunkClient::TChunkId>()(value.ChunkId) * 497 + value.MediumIndex;
    }
};

#define SESSION_ID_INL_H_
#include "session_id-inl.h"
#undef SESSION_ID_INL_H_
