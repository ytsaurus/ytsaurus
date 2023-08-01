#pragma once

#include <yt/yt/server/node/data_node/artifact.pb.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/object_client/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TArtifactKey
    : public NProto::TArtifactKey
{
    TArtifactKey() = default;

    explicit TArtifactKey(NChunkClient::TChunkId id);
    explicit TArtifactKey(const NControllerAgent::NProto::TFileDescriptor& descriptor);

    i64 GetCompressedDataSize() const;

    // Hasher.
    operator size_t() const;

    // Comparer.
    bool operator == (const TArtifactKey& other) const;
};

TString ToString(const TArtifactKey& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

