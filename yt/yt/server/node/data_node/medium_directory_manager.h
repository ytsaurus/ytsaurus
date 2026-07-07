#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

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
    TAtomicIntrusivePtr<NChunkClient::TMediumDirectory> MediumDirectory_;
};

DEFINE_REFCOUNTED_TYPE(TMediumDirectoryManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
