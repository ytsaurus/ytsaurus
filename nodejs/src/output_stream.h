#pragma once

#include "stream_base.h"

#include <yt/core/actions/future.h>

#include <util/system/mutex.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

//! This class adheres to IOutputStream interface as a C++ object and
//! simultaneously provides 'readable stream' interface stubs as a JS object
//! thus effectively acting as a bridge from C++ to JS.
class TOutputStreamWrap
    : public TNodeJSStreamBase
    , public IOutputStream
    , public TRefTracked<TOutputStreamWrap>
{
protected:
    TOutputStreamWrap(ui64 watermark);
    ~TOutputStreamWrap() throw();

public:
    static v8::Persistent<v8::FunctionTemplate> ConstructorTemplate;
    static void Initialize(v8::Handle<v8::Object> target);
    static bool HasInstance(v8::Handle<v8::Value> value);

    // Synchronous JS API.
    static v8::Handle<v8::Value> New(const v8::Arguments& args);

    static v8::Handle<v8::Value> Pull(const v8::Arguments& args);
    v8::Handle<v8::Value> DoPull();

    static v8::Handle<v8::Value> Destroy(const v8::Arguments& args);
    void DoDestroy();

    static v8::Handle<v8::Value> Drain(const v8::Arguments& args);
    v8::Handle<v8::Value> DoDrain();

    static v8::Handle<v8::Value> SetMaxPartCount(const v8::Arguments& args);
    static v8::Handle<v8::Value> SetMaxPartLength(const v8::Arguments& args);

    // Diagnostics.
    const ui64 GetBytesEnqueued() const;
    const ui64 GetBytesDequeued() const;

    void MarkAsFinishing();
    bool IsFinished() const;

protected:
    // C++ API.
    virtual void DoWrite(const void* buffer, size_t length) override;
    virtual void DoWriteV(const TPart* parts, size_t count) override;
    virtual void DoFinish() override;

private:
    struct TOutputPart
    {
        size_t Length = 0;
        size_t RefCount = 0;

        char* Buffer()
        {
            return (char*)(this + 1);
        }
    };

    bool CanFlow() const;
    void RunFlow();
    static int AsyncOnFlowing(eio_req* request);

    void ProtectedUpdateAndNotifyWriter(std::function<void()> mutator);
    void PushToQueue(std::unique_ptr<char[]> holder, size_t length);

    static void DeleteCallback(char* buffer, void* hint);

private:
    const ui64 Watermark_;

    size_t MaxPartCount_ = 8;
    size_t MaxPartLength_ = 1_GB - 1;

    std::atomic<bool> FlowEstablished_ = {false};

    // Protects everything below.
    TMutex Mutex_;

    bool IsFinishing_ = false;
    bool IsFinished_ = false;
    bool IsDestroyed_ = false;

    ui64 BytesInFlight_ = 0;
    ui64 BytesEnqueued_ = 0;
    ui64 BytesDequeued_ = 0;

    TPromise<void> WritePromise_;

    std::deque<std::unique_ptr<char[]>> Queue_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
