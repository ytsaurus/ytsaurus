#pragma once

#include "private.h"

#include <yt/core/logging/log.h>

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

class TCompletionQueueTag
{
public:
    void* GetTag(int cookie = 0);
    virtual void Run(bool success, int cookie) = 0;

protected:
    virtual ~TCompletionQueueTag() = default;

};

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
{
public:
    TDispatcher();
    ~TDispatcher();

    static TDispatcher* Get();
    static void StaticShutdown();

    TGrpcLibraryLockPtr CreateLibraryLock();
    grpc_completion_queue* PickRandomCompletionQueue();

private:
    friend class TGrpcLibraryLock;

    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcLibraryLock
    : public TIntrinsicRefCounted
{
private:
    DECLARE_NEW_FRIEND();
    friend class TDispatcher::TImpl;

    TGrpcLibraryLock();
    ~TGrpcLibraryLock();
};

DEFINE_REFCOUNTED_TYPE(TGrpcLibraryLock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
