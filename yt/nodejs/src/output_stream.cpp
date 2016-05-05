#include "output_stream.h"

#include <yt/core/misc/error.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

namespace {

static const int MaxPartsPerPull = 8;

static Persistent<String> OnDataSymbol;

void DeleteCallback(char* data, void* hint)
{
    // God bless the C++ compiler.
    const int hintAsInt = static_cast<int>(reinterpret_cast<size_t>(hint));
    v8::V8::AdjustAmountOfExternalAllocatedMemory(-hintAsInt);
    delete[] data;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Persistent<FunctionTemplate> TOutputStreamWrap::ConstructorTemplate;

TOutputStreamWrap::TOutputStreamWrap(ui64 lowWatermark, ui64 highWatermark)
    : TNodeJSStreamBase()
    , LowWatermark_(lowWatermark)
    , HighWatermark_(highWatermark)
{
    THREAD_AFFINITY_IS_V8();
}

TOutputStreamWrap::~TOutputStreamWrap() throw()
{
    THREAD_AFFINITY_IS_V8();

    DisposeBuffers();
}

////////////////////////////////////////////////////////////////////////////////

void TOutputStreamWrap::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    OnDataSymbol = NODE_PSYMBOL("on_data");

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TOutputStreamWrap::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TOutputStreamWrap"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Pull", TOutputStreamWrap::Pull);

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Drain", TOutputStreamWrap::Drain);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Destroy", TOutputStreamWrap::Destroy);

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "IsEmpty", TOutputStreamWrap::IsEmpty);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "IsDestroyed", TOutputStreamWrap::IsDestroyed);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "IsPaused", TOutputStreamWrap::IsPaused);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "IsCompleted", TOutputStreamWrap::IsCompleted);

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

    YCHECK(args.Length() == 2);

    EXPECT_THAT_IS(args[0], Uint32);
    EXPECT_THAT_IS(args[1], Uint32);

    ui64 lowWatermark = args[0]->Uint32Value();
    ui64 highWatermark = args[1]->Uint32Value();

    TOutputStreamWrap* stream = nullptr;
    try {
        stream = new TOutputStreamWrap(lowWatermark, highWatermark);
        stream->Wrap(args.This());

        stream->handle_->Set(
            String::NewSymbol("low_watermark"),
            Integer::NewFromUnsigned(lowWatermark),
            (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));
        stream->handle_->Set(
            String::NewSymbol("high_watermark"),
            Integer::NewFromUnsigned(highWatermark),
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
    TOutputStreamWrap* stream =
        ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    return scope.Close(stream->DoPull());
}

Handle<Value> TOutputStreamWrap::DoPull()
{
    THREAD_AFFINITY_IS_V8();

    if (IsDestroyed_.load(std::memory_order_relaxed)) {
        return Undefined();
    }

    TOutputPart part;
    Local<Array> parts = Array::New(MaxPartsPerPull);

    for (int i = 0; i < MaxPartsPerPull; ++i) {
        if (!Queue_.Dequeue(&part)) {
            break;
        }

        node::Buffer* buffer = node::Buffer::New(
            part.Buffer,
            part.Length,
            DeleteCallback,
            (void*)part.Length);
        parts->Set(i, buffer->handle_);

        v8::V8::AdjustAmountOfExternalAllocatedMemory(+(int)part.Length);

        BytesDequeued_.fetch_add(part.Length);
        auto transientSize = BytesInFlight_.fetch_sub(part.Length);
        if (transientSize - part.Length < LowWatermark_ && LowWatermark_ <= transientSize) {
            Conditional_.NotifyAll();
        }
    }

    return parts;
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamWrap::Drain(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TOutputStreamWrap* stream =
        ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    stream->DoDrain();

    return Undefined();
}

void TOutputStreamWrap::DoDrain()
{
    THREAD_AFFINITY_IS_V8();

    YASSERT(!IsDestroyed_.load(std::memory_order_relaxed));

    IgniteOnData();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamWrap::Destroy(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TOutputStreamWrap* stream =
        ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    stream->DoDestroy();

    return Undefined();
}

void TOutputStreamWrap::DoDestroy()
{
    THREAD_AFFINITY_IS_V8();

    IsDestroyed_ = false;
    IsPaused_ = false;

    Conditional_.NotifyAll();

    DisposeBuffers();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TOutputStreamWrap::IsEmpty(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TOutputStreamWrap* stream =
        ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    return scope.Close(Boolean::New(stream->Queue_.IsEmpty()));
}

Handle<Value> TOutputStreamWrap::IsDestroyed(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TOutputStreamWrap* stream =
        ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    return scope.Close(Boolean::New(stream->IsDestroyed_.load(std::memory_order_relaxed)));
}

Handle<Value> TOutputStreamWrap::IsPaused(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TOutputStreamWrap* stream =
        ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    return scope.Close(Boolean::New(stream->IsPaused_.load(std::memory_order_relaxed)));
}

Handle<Value> TOutputStreamWrap::IsCompleted(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TOutputStreamWrap* stream =
        ObjectWrap::Unwrap<TOutputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    return scope.Close(Boolean::New(stream->IsCompleted_.load(std::memory_order_relaxed)));
}

////////////////////////////////////////////////////////////////////////////////

int TOutputStreamWrap::AsyncOnData(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();

    TOutputStreamWrap* stream = static_cast<TOutputStreamWrap*>(request->data);
    node::MakeCallback(stream->handle_, OnDataSymbol, 0, nullptr);

    stream->AsyncUnref();

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

void TOutputStreamWrap::DoWrite(const void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    if (data == nullptr || length == 0) {
        return;
    }

    WritePrologue();

    char* buffer = new char[length];
    YASSERT(buffer);

    ::memcpy(buffer, data, length);

    WriteEpilogue(buffer, length);
}

void TOutputStreamWrap::DoWriteV(const TPart* parts, size_t count)
{
    THREAD_AFFINITY_IS_ANY();

    if (parts == nullptr || count == 0) {
        return;
    }

    WritePrologue();

    size_t offset = 0;
    size_t length = 0;

    for (size_t i = 0; i < count; ++i) {
        length += parts[i].len;
    }

    char* buffer = new char[length];
    YASSERT(buffer);

    for (size_t i = 0; i < count; ++i) {
        const auto& part = parts[i];
        ::memcpy(buffer + offset, part.buf, part.len);
        offset += part.len;
    }

    WriteEpilogue(buffer, length);
}

void TOutputStreamWrap::WritePrologue()
{
    Conditional_.Await([&] () -> bool {
        return
            IsDestroyed_.load(std::memory_order_relaxed) ||
            IsCompleted_.load(std::memory_order_relaxed) ||
            BytesInFlight_.load(std::memory_order_relaxed) < HighWatermark_;
    });

    if (IsDestroyed_.load(std::memory_order_relaxed)) {
        THROW_ERROR_EXCEPTION("TOutputStreamWrap was terminated");
    }
}

void TOutputStreamWrap::WriteEpilogue(char* buffer, size_t length)
{
    TOutputPart part;
    part.Buffer = buffer;
    part.Length = length;
    Queue_.Enqueue(part);

    BytesEnqueued_.fetch_add(length);
    BytesInFlight_.fetch_add(length);

    // We require that calling party holds a synchronous lock on the stream.
    // In case of TDriverWrap an instance TNodeJSInputStack holds a lock
    // and TDriverWrap implementation guarantees that all Write() calls
    // are within scope of the lock.
    EmitAndStifleOnData();
}

////////////////////////////////////////////////////////////////////////////////

void TOutputStreamWrap::DisposeBuffers()
{
    TOutputPart part;
    while (Queue_.Dequeue(&part)) {
        DeleteCallback(part.Buffer, nullptr);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
