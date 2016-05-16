#pragma once

#include "common.h"

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class IAsyncRefCounted
{
protected:
    IAsyncRefCounted();
    virtual ~IAsyncRefCounted();

    IAsyncRefCounted(const IAsyncRefCounted&) = delete;
    IAsyncRefCounted(IAsyncRefCounted&&) = delete;

    void AsyncRef();
    void AsyncUnref();

    virtual void SyncRef() = 0;
    virtual void SyncUnref() = 0;

    int GetAsyncRefCount() const;

    friend void Ref(IAsyncRefCounted*);
    friend void Unref(IAsyncRefCounted*);

private:
    std::atomic<int> AsyncRefCounter_ = {0};
};

void Ref(IAsyncRefCounted* object);
void Unref(IAsyncRefCounted* object);

////////////////////////////////////////////////////////////////////////////////

class TAsyncRefCountedObjectWrap
    : public node::ObjectWrap
    , public IAsyncRefCounted
{
protected:
    TAsyncRefCountedObjectWrap();
    virtual ~TAsyncRefCountedObjectWrap();

    virtual void SyncRef() override;
    virtual void SyncUnref() override;

private:
    pthread_t HomeThread_;
    bool IsHomeThread() const;

    static int DelayedSyncUnref(eio_req* request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT

