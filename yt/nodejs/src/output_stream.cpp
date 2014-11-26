#include "output_stream.h"

#include <core/misc/error.h>

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
    , IsDestroyed_(0)
    , IsPaused_(0)
    , IsCompleted_(0)
    , BytesInFlight(0)
    , BytesEnqueued(0)
    , BytesDequeued(0)
    , LowWatermark(lowWatermark)
    , HighWatermark(highWatermark)
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

    TOutputStreamWrap* stream = NULL;
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

    if (AtomicGet(IsDestroyed_)) {
        return Undefined();
    }

    TOutputPart part;
    Local<Array> parts = Array::New(MaxPartsPerPull);

    for (int i = 0; i < MaxPartsPerPull; ++i) {
        if (!Queue.Dequeue(&part)) {
            break;
        }

        node::Buffer* buffer = node::Buffer::New(
            part.Buffer,
            part.Length,
            DeleteCallback,
            (void*)part.Length);
        parts->Set(i, buffer->handle_);

        v8::V8::AdjustAmountOfExternalAllocatedMemory(+(int)part.Length);

        AtomicAdd(BytesDequeued, part.Length);
        auto transientSize = AtomicSub(BytesInFlight, part.Length);
        if (transientSize < LowWatermark && LowWatermark <= transientSize + part.Length) {
            Conditional.NotifyAll();
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

    YASSERT(!AtomicGet(IsDestroyed_));

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

    AtomicSet(IsDestroyed_, 1);
    AtomicSet(IsPaused_, 0);

    Conditional.NotifyAll();

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
    return scope.Close(Boolean::New(stream->Queue.IsEmpty()));
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
    return scope.Close(Boolean::New(AtomicGet(stream->IsDestroyed_)));
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
    return scope.Close(Boolean::New(AtomicGet(stream->IsPaused_)));
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
    return scope.Close(Boolean::New(AtomicGet(stream->IsCompleted_)));
}

////////////////////////////////////////////////////////////////////////////////

int TOutputStreamWrap::AsyncOnData(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();

    TOutputStreamWrap* stream = static_cast<TOutputStreamWrap*>(request->data);
    node::MakeCallback(stream->handle_, OnDataSymbol, 0, NULL);

    stream->AsyncUnref();

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

void TOutputStreamWrap::DoWrite(const void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    if (data == NULL || length == 0) {
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

    if (parts == NULL || count == 0) {
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
    Conditional.Await([&] () -> bool {
        return
            AtomicGet(IsDestroyed_) ||
            AtomicGet(IsCompleted_) ||
            (AtomicGet(BytesInFlight) < HighWatermark);
    });

    if (AtomicGet(IsDestroyed_)) {
        THROW_ERROR_EXCEPTION("TOutputStreamWrap was terminated");
    }
}

void TOutputStreamWrap::WriteEpilogue(char* buffer, size_t length)
{
    TOutputPart part;
    part.Buffer = buffer;
    part.Length = length;
    Queue.Enqueue(part);

    AtomicAdd(BytesInFlight, length);
    AtomicAdd(BytesEnqueued, length);

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
    while (Queue.Dequeue(&part)) {
        DeleteCallback(part.Buffer, NULL);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
