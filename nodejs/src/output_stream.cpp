#include "output_stream.h"

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

static Persistent<String> OnFlowingSymbol;

} // namespace

////////////////////////////////////////////////////////////////////////////////

Persistent<FunctionTemplate> TOutputStreamWrap::ConstructorTemplate;

TOutputStreamWrap::TOutputStreamWrap(ui64 watermark)
    : TNodeJSStreamBase()
    , Watermark_(watermark)
{
    THREAD_AFFINITY_IS_V8();
}

TOutputStreamWrap::~TOutputStreamWrap()
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

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Drain", TOutputStreamWrap::Drain);

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "SetMaxPartCount", TOutputStreamWrap::SetMaxPartCount);

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "SetMaxPartLength", TOutputStreamWrap::SetMaxPartLength);

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

    try {
        ui64 watermark = args[0]->Uint32Value();

        auto stream = new TOutputStreamWrap(watermark);
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

    Local<Array> parts = Array::New(MaxPartCount_);

    // Short-path for destroyed streams.

    ProtectedUpdateAndNotifyWriter([&] () {
        for (int i = 0; i < MaxPartCount_; /**/) {
            if (Queue_.empty()) {
                break;
            }

            auto holder = std::move(Queue_.front());
            Queue_.pop_front();

            auto* header = reinterpret_cast<TOutputPart*>(holder.get());
            YCHECK(header->Length > 0);
            YCHECK(header->RefCount == 0);

            auto* buffer = header->Buffer();
            auto length = header->Length;

            while (length > 0) {
                auto partLength = length > MaxPartLength_ ? MaxPartLength_ : length;
                auto* object = node::Buffer::New(
                    buffer,
                    partLength,
                    DeleteCallback,
                    (void*)header);

                parts->Set(i, object->handle_);
                ++i;
                ++header->RefCount;

                v8::V8::AdjustAmountOfExternalAllocatedMemory(partLength);

                BytesDequeued_ += partLength;
                BytesInFlight_ -= partLength;

                buffer += partLength;
                length -= partLength;
            }

            if (header->RefCount > 0) {
                holder.release();
            }
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

    return scope.Close(Undefined());
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

Handle<Value> TOutputStreamWrap::Drain(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* stream = ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    return scope.Close(stream->DoDrain());
}

Handle<Value> TOutputStreamWrap::DoDrain()
{
    THREAD_AFFINITY_IS_V8();

    FlowEstablished_.store(false);

    auto guard = Guard(Mutex_);

    return Boolean::New(IsFinished_ && Queue_.empty());
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamWrap::SetMaxPartCount(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 1);
    EXPECT_THAT_IS(args[0], Uint32);

    auto* stream = ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());
    auto value = args[0]->Uint32Value();
    YCHECK(value > 0 && value < 16);
    stream->MaxPartCount_ = value;

    return scope.Close(Undefined());
}

Handle<Value> TOutputStreamWrap::SetMaxPartLength(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 1);
    EXPECT_THAT_IS(args[0], Uint32);

    auto* stream = ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());
    auto value = args[0]->Uint32Value();
    YCHECK(value > 0 && value < 1_GB);
    stream->MaxPartLength_ = value;

    return scope.Close(Undefined());
}

////////////////////////////////////////////////////////////////////////////////

bool TOutputStreamWrap::CanFlow() const
{
    return
        IsFinishing_ || IsFinished_ || IsDestroyed_ ||
        BytesInFlight_ < Watermark_;
}

void TOutputStreamWrap::RunFlow()
{
    bool expected = false;
    if (FlowEstablished_.compare_exchange_strong(expected, true, std::memory_order_acquire)) {
        AsyncRef();
        EIO_PUSH(TOutputStreamWrap::AsyncOnFlowing, this);
    }
}

int TOutputStreamWrap::AsyncOnFlowing(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    auto* stream = static_cast<TOutputStreamWrap*>(request->data);
    node::MakeCallback(stream->handle_, OnFlowingSymbol, 0, nullptr);
    stream->AsyncUnref();

    return 0;
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
    THREAD_AFFINITY_IS_ANY();

    ProtectedUpdateAndNotifyWriter([&] () {
        IsFinishing_ = true;
    });
}

bool TOutputStreamWrap::IsFinished() const
{
    auto guard = Guard(Mutex_);
    return IsFinished_;
}

void TOutputStreamWrap::DoWrite(const void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    if (data == nullptr || length == 0) {
        return;
    }

    TPart part{data, length};
    DoWriteV(&part, 1);
}

void TOutputStreamWrap::DoWriteV(const TPart* parts, size_t count)
{
    THREAD_AFFINITY_IS_ANY();

    if (parts == nullptr || count == 0) {
        return;
    }

    size_t offset = 0;
    size_t length = 0;

    for (size_t i = 0; i < count; ++i) {
        length += parts[i].len;
    }

    std::unique_ptr<char[]> holder(new char[sizeof(TOutputPart) + length]);
    auto* header = reinterpret_cast<TOutputPart*>(holder.get());
    auto* buffer = reinterpret_cast<char*>(header + 1);

    header->Length = length;
    header->RefCount = 0;

    for (size_t i = 0; i < count; ++i) {
        const auto& part = parts[i];
        ::memcpy(buffer + offset, part.buf, part.len);
        offset += part.len;
    }

    PushToQueue(std::move(holder), length);
}

void TOutputStreamWrap::DoFinish()
{
    THREAD_AFFINITY_IS_ANY();

    ProtectedUpdateAndNotifyWriter([&] () {
        IsFinishing_ = true;
        IsFinished_ = true;
    });

    RunFlow();
}

void TOutputStreamWrap::ProtectedUpdateAndNotifyWriter(std::function<void()> mutator)
{
    THREAD_AFFINITY_IS_ANY();

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
        writePromise.Set();
    }
}

void TOutputStreamWrap::PushToQueue(std::unique_ptr<char[]> holder, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    TIntrusivePtr<IAsyncRefCounted> ref(this);

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

    Queue_.emplace_back(std::move(holder));

    BytesEnqueued_ += length;
    BytesInFlight_ += length;

    RunFlow();
}

void TOutputStreamWrap::DeleteCallback(char* buffer, void* hint)
{
    auto* header = reinterpret_cast<TOutputPart*>(hint);
    YCHECK(buffer >= header->Buffer() && buffer <= header->Buffer() + header->Length);
    YCHECK(header->RefCount > 0);
    v8::V8::AdjustAmountOfExternalAllocatedMemory(-header->Length);
    if (--header->RefCount == 0) {
        delete[] reinterpret_cast<char*>(header);
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
