#pragma once

#include "stream_base.h"

#include <yt/core/actions/future.h>

#include <util/system/mutex.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

//! This class adheres to IInputStream interface as a C++ object and
//! simultaneously provides 'writable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from JS to C++.
class TInputStreamWrap
    : public TNodeJSStreamBase
    , public IInputStream
    , public TRefTracked<TInputStreamWrap>
{
protected:
    TInputStreamWrap(ui64 lowWatermark, ui64 highWatermark);
    ~TInputStreamWrap();

public:
    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    static v8::Handle<v8::Value> Push(const v8::Arguments& args);
    bool DoPush(v8::Persistent<v8::Value> handle, char* data, size_t offset, size_t length);

    static v8::Handle<v8::Value> End(const v8::Arguments& args);
    void DoEnd();

    static v8::Handle<v8::Value> Destroy(const v8::Arguments& args);
    void DoDestroy();

    // Asynchronous JS API.
    static int AsyncSweep(eio_req* request);
    void EnqueueSweep();
    void DoSweep();

    static int AsyncDrain(eio_req* request);
    void EnqueueDrain();

    // Diagnostics.
    const ui64 GetBytesEnqueued() const;
    const ui64 GetBytesDequeued() const;

protected:
    // C++ API.
    size_t DoRead(void* data, size_t length) override;

private:
    void ProtectedUpdateAndNotifyReader(std::function<void()> mutator);
    void DisposeStream();
    void DisposeHandles(std::deque<std::unique_ptr<TInputPart>>* queue);

private:
    const ui64 LowWatermark_;
    const ui64 HighWatermark_;

    std::atomic<bool> SweepRequestPending_ = {false};
    std::atomic<bool> DrainRequestPending_ = {false};

    // Protects everything below.
    TMutex Mutex_;

    bool IsPushable_ = true;
    bool IsReadable_ = true;

    ui64 BytesInFlight_ = 0;
    ui64 BytesEnqueued_ = 0;
    ui64 BytesDequeued_ = 0;

    TPromise<void> ReadPromise_;
    std::deque<std::unique_ptr<TInputPart>> ActiveQueue_;
    std::deque<std::unique_ptr<TInputPart>> InactiveQueue_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
