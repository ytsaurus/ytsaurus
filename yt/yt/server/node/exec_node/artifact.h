#pragma once

#include <yt/yt/server/node/exec_node/artifact.pb.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/object_client/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TArtifactKey
    : public NProto::TArtifactKey
{
    TArtifactKey() = default;

    explicit TArtifactKey(NChunkClient::TChunkId id);
    explicit TArtifactKey(const NControllerAgent::NProto::TFileDescriptor& descriptor);

    i64 GetCompressedDataSize() const;
    i64 GetUncompressedDataSize() const;

    // Hasher.
    operator size_t() const;

    // Comparer.
    bool operator == (const TArtifactKey& other) const;
};

void FormatValue(TStringBuilderBase* builder, const TArtifactKey& key, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode

