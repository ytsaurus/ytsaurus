#pragma once

#include "private.h"

#include <yt/yt/core/misc/singleton.h>

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
    static TDispatcher* Get();

    TGrpcLibraryLockPtr CreateLibraryLock();
    grpc_completion_queue* PickRandomCompletionQueue();

private:
    DECLARE_LEAKY_SINGLETON_FRIEND()
    friend class TGrpcLibraryLock;

    TDispatcher();
    ~TDispatcher();

    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcLibraryLock
    : public TRefCounted
{
private:
    DECLARE_NEW_FRIEND();
    friend class TDispatcher::TImpl;

    TGrpcLibraryLock();
    ~TGrpcLibraryLock();
};

DEFINE_REFCOUNTED_TYPE(TGrpcLibraryLock)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
