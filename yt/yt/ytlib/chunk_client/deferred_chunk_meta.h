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

    //! Register #callback that will be called after meta is finalized.
    //! This can be used to transfer meta directly to the subscriber.
    void SubscribeMetaFinalized(TCallback<void(const TRefCountedChunkMeta*)> callback);

    //! Should be called exactly once; invokes all the registered finalizers.
    void Finalize();

    //! True if #Finalize was called.
    bool IsFinalized() const;

private:
    std::vector<std::function<void(TDeferredChunkMeta*)>> Finalizers_;
    std::vector<TCallback<void(const TRefCountedChunkMeta*)>> FinalizationSubscribers_;

    bool Finalized_ = false;
};

DEFINE_REFCOUNTED_TYPE(TDeferredChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
