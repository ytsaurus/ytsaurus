#pragma once

#include "public.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/ytlib/chunk_client/proto/session_id.pb.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TSessionId
{
    TSessionId();
    TSessionId(TChunkId chunkId, int mediumIndex);

    bool operator==(const TSessionId& other) const = default;

    TChunkId ChunkId;
    int MediumIndex;
};

void FormatValue(TStringBuilderBase* builder, TSessionId id, TStringBuf format);
TString ToString(TSessionId id);

void ToProto(NProto::TSessionId* protoSessionId, TSessionId sessionId);
void FromProto(TSessionId* sessionId, const NProto::TSessionId& protoSessionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

Y_DECLARE_PODTYPE(NYT::NChunkClient::TSessionId);

template <>
struct THash<NYT::NChunkClient::TSessionId>
{
    size_t operator()(NYT::NChunkClient::TSessionId value) const;
};
