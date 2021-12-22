#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TMediumDirectoryManager
    : public TRefCounted
{
public:
    TMediumDirectoryManager(
        IBootstrap* bootstrap,
        const NLogging::TLogger& logger);

    NChunkClient::TMediumDirectoryPtr GetMediumDirectory() const;

    void UpdateMediumDirectory(const NChunkClient::NProto::TMediumDirectory& mediumDirectory);

private:
    IBootstrap* Bootstrap_;
    const NLogging::TLogger Logger;
    NChunkClient::TMediumDirectoryPtr MediumDirectory_;
};

DEFINE_REFCOUNTED_TYPE(TMediumDirectoryManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
