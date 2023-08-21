#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Simple helper subclass of TRefCountedChunkMeta which allows registration of arbitrary deferred
//! callbacks cusomizing meta right before its finalization (e.g. block index renumeration).
class TDeferredChunkMeta
    : public TRefCountedChunkMeta
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::optional<std::vector<int>>, BlockIndexMapping);

public:
    //! Register #finalizer callback that will be invoked during finalization.
    void RegisterFinalizer(std::function<void(TDeferredChunkMeta*)> finalizer);

    //! Should be called exactly once; invokes all the registered finalizers.
    void Finalize();

    //! True if #Finalize was called.
    bool IsFinalized() const;

private:
    std::vector<std::function<void(TDeferredChunkMeta*)>> Finalizers_;
    bool Finalized_ = false;
};

DEFINE_REFCOUNTED_TYPE(TDeferredChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
