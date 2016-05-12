#include "output_stream.h"

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

static const int MaxPartsPerPull = 8;

static Persistent<String> OnFlowingSymbol;

void DeleteCallback(char* buffer, void* hint)
{
    const size_t length = (size_t)hint;
    v8::V8::AdjustAmountOfExternalAllocatedMemory(-length);
    delete[] buffer;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Persistent<FunctionTemplate> TOutputStreamWrap::ConstructorTemplate;

TOutputStreamWrap::TOutputStreamWrap(ui64 watermark)
    : TNodeJSStreamBase()
    , Watermark_(watermark)
{
    THREAD_AFFINITY_IS_V8();
}

TOutputStreamWrap::~TOutputStreamWrap() throw()
{
    THREAD_AFFINITY_IS_V8();
}

////////////////////////////////////////////////////////////////////////////////

void TOutputStreamWrap::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    OnFlowingSymbol = NODE_PSYMBOL("on_flowing");

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TOutputStreamWrap::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TOutputStreamWrap"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Pull", TOutputStreamWrap::Pull);

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Destroy", TOutputStreamWrap::Destroy);

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "IsFlowing", TOutputStreamWrap::IsFlowing);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "IsFinished", TOutputStreamWrap::IsFinished);

    target->Set(
        String::NewSymbol("TOutputStreamWrap"),
        ConstructorTemplate->GetFunction());
}

bool TOutputStreamWrap::HasInstance(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return
        value->IsObject() &&
        ConstructorTemplate->HasInstance(value->ToObject());
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamWrap::New(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 1);

    EXPECT_THAT_IS(args[0], Uint32);

    ui64 watermark = args[0]->Uint32Value();

    TOutputStreamWrap* stream = nullptr;
    try {
        stream = new TOutputStreamWrap(watermark);
        stream->Wrap(args.This());

        stream->handle_->Set(
            String::NewSymbol("watermark"),
            Integer::NewFromUnsigned(watermark),
            (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));
        stream->handle_->Set(
            String::NewSymbol("cxx_id"),
            Integer::NewFromUnsigned(stream->Id_),
            (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));

        return scope.Close(args.This());
    } catch (const std::exception& ex) {
        if (stream) {
            delete stream;
        }

        return ThrowException(Exception::Error(String::New(ex.what())));
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamWrap::Pull(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* stream = ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    return scope.Close(stream->DoPull());
}

Handle<Value> TOutputStreamWrap::DoPull()
{
    THREAD_AFFINITY_IS_V8();

    Local<Array> parts = Array::New(MaxPartsPerPull);
    size_t count = 0;

    ProtectedUpdateAndNotifyWriter([&] () {
        YCHECK(IsFlowing_);

        for (int i = 0; i < MaxPartsPerPull; ++i) {
            if (Queue_.empty()) {
                break;
            }

            auto part = std::move(Queue_.front());
            Queue_.pop_front();

            YCHECK(static_cast<bool>(part));

            auto* buffer = node::Buffer::New(
                part.Buffer.release(),
                part.Length,
                DeleteCallback,
                (void*)part.Length);

            parts->Set(i, buffer->handle_);
            ++count;

            v8::V8::AdjustAmountOfExternalAllocatedMemory(part.Length);

            BytesDequeued_ += part.Length;
            BytesInFlight_ -= part.Length;
        }

        if (count == 0) {
            AsyncUnref();
            IsFlowing_ = false;
        }
    });

    return parts;
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamWrap::Destroy(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* stream = ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    stream->DoDestroy();

    return Undefined();
}

void TOutputStreamWrap::DoDestroy()
{
    THREAD_AFFINITY_IS_V8();

    ProtectedUpdateAndNotifyWriter([&] () {
        IsDestroyed_ = true;

        Queue_.clear();
    });
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamWrap::IsFlowing(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* stream = ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    return scope.Close(stream->DoIsFlowing());
}

Handle<Value> TOutputStreamWrap::DoIsFlowing()
{
    THREAD_AFFINITY_IS_V8();
    auto guard = Guard(Mutex_);
    return Boolean::New(IsFlowing_);
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamWrap::IsFinished(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* stream = ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    return scope.Close(stream->DoIsFinished());
}

Handle<Value> TOutputStreamWrap::DoIsFinished()
{
    THREAD_AFFINITY_IS_V8();
    auto guard = Guard(Mutex_);
    return Boolean::New(IsFinished_);
}

////////////////////////////////////////////////////////////////////////////////

int TOutputStreamWrap::AsyncOnFlowing(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    auto* stream = static_cast<TOutputStreamWrap*>(request->data);
    node::MakeCallback(stream->handle_, OnFlowingSymbol, 0, nullptr);

    return 0;
}

void TOutputStreamWrap::Flow(bool withinV8)
{
    if (!IsFlowing_) {
        IsFlowing_ = true;
        AsyncRef(withinV8);
        EIO_PUSH(TOutputStreamWrap::AsyncOnFlowing, this);
    }
}

////////////////////////////////////////////////////////////////////////////////

const ui64 TOutputStreamWrap::GetBytesEnqueued() const
{
    auto guard = Guard(Mutex_);
    return BytesEnqueued_;
}

const ui64 TOutputStreamWrap::GetBytesDequeued() const
{
    auto guard = Guard(Mutex_);
    return BytesDequeued_;
}

void TOutputStreamWrap::MarkAsFinishing()
{
    ProtectedUpdateAndNotifyWriter([&] () {
        IsFinishing_ = true;
    });
}

void TOutputStreamWrap::DoWrite(const void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    if (data == nullptr || length == 0) {
        return;
    }

    TScopedRef<false> guardAsyncRef(this);

    std::unique_ptr<char[]> buffer(new char[length]);

    ::memcpy(&buffer[0], data, length);

    PushToQueue(std::move(buffer), length);
}

void TOutputStreamWrap::DoWriteV(const TPart* parts, size_t count)
{
    THREAD_AFFINITY_IS_ANY();

    if (parts == nullptr || count == 0) {
        return;
    }

    TScopedRef<false> guardAsyncRef(this);

    size_t offset = 0;
    size_t length = 0;

    for (size_t i = 0; i < count; ++i) {
        length += parts[i].len;
    }

    std::unique_ptr<char[]> buffer(new char[length]);

    for (size_t i = 0; i < count; ++i) {
        const auto& part = parts[i];
        ::memcpy(&buffer[offset], part.buf, part.len);
        offset += part.len;
    }

    PushToQueue(std::move(buffer), length);
}

void TOutputStreamWrap::DoFinish()
{
    auto guard = Guard(Mutex_);
    IsFinishing_ = true;
    IsFinished_ = true;

    // We require that calling party holds a synchronous lock on the stream.
    // In case of TDriverWrap an instance TNodeJSInputStack holds a lock
    // and TDriverWrap implementation guarantees that all Write() calls
    // are within scope of the lock.
    Flow(false);
}

bool TOutputStreamWrap::CanFlow() const
{
    return
        IsFinishing_ || IsFinished_ || IsDestroyed_ ||
        BytesInFlight_ < Watermark_;
}

void TOutputStreamWrap::ProtectedUpdateAndNotifyWriter(std::function<void()> mutator)
{
    TPromise<void> writePromise;
    {
        auto guard = Guard(Mutex_);
        mutator();
        if (WritePromise_) {
            if (CanFlow()) {
                writePromise = std::move(WritePromise_);
            }
        }
    }
    if (writePromise) {
        WritePromise_.Set();
    }
}

void TOutputStreamWrap::PushToQueue(std::unique_ptr<char[]> buffer, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    auto guard = Guard(Mutex_);

    // This bit should be set once we
    YCHECK(!IsFinished_);

    if (!CanFlow()) {
        YCHECK(!WritePromise_);
        WritePromise_ = NewPromise<void>();

        auto writePromise = WritePromise_;

        {
            auto unguard = Unguard(Mutex_);
            WaitFor(writePromise.ToFuture())
                .ThrowOnError();
        }
    }

    if (IsDestroyed_) {
        THROW_ERROR_EXCEPTION("TOutputStreamWrap was terminated");
    }

    Queue_.emplace_back(std::move(buffer), length);

    BytesEnqueued_ += length;
    BytesInFlight_ += length;

    // We require that calling party holds a synchronous lock on the stream.
    // In case of TDriverWrap an instance TNodeJSInputStack holds a lock
    // and TDriverWrap implementation guarantees that all Write() calls
    // are within scope of the lock.
    Flow(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
