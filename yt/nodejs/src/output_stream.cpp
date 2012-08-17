#include "output_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

namespace {

static Persistent<String> OnWriteSymbol;
static Persistent<String> OnDrainSymbol;

void DeleteCallback(char* data, void* hint)
{
    delete[] data;
}

} // namespace

Persistent<FunctionTemplate> TNodeJSOutputStream::ConstructorTemplate;

////////////////////////////////////////////////////////////////////////////////


TNodeJSOutputStream::TNodeJSOutputStream()
    : TNodeJSStreamBase()
    , IsWritable(1)
    , WriteRequestPending(0)
    , FlushRequestPending(0)
    , FinishRequestPending(0)
{
    THREAD_AFFINITY_IS_V8();
}

TNodeJSOutputStream::~TNodeJSOutputStream() throw()
{
    THREAD_AFFINITY_IS_V8();

    // XXX(sandello): Maybe add diagnostics about deleting non-empty queue?
    DisposeBuffers();
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    OnWriteSymbol = NODE_PSYMBOL("on_write");
    OnDrainSymbol = NODE_PSYMBOL("on_drain");

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TNodeJSOutputStream::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TNodeJSOutputStream"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Destroy", TNodeJSOutputStream::Destroy);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "IsEmpty", TNodeJSOutputStream::IsEmpty);

    target->Set(
        String::NewSymbol("TNodeJSOutputStream"),
        ConstructorTemplate->GetFunction());
}

bool TNodeJSOutputStream::HasInstance(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return
        value->IsObject() &&
        ConstructorTemplate->HasInstance(value->ToObject());
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSOutputStream::New(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YASSERT(args.Length() == 0);

    TNodeJSOutputStream* stream = NULL;
    try {
        stream = new TNodeJSOutputStream();
        stream->Wrap(args.This());

        return scope.Close(args.This());
    } catch (const std::exception& ex) {
        if (stream) {
            delete stream;
        }

        return ThrowException(Exception::Error(String::New(ex.what())));
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSOutputStream::Destroy(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSOutputStream* stream =
        ObjectWrap::Unwrap<TNodeJSOutputStream>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 0);

    // Do the work.
    return scope.Close(stream->DoDestroy());
}

Handle<Value> TNodeJSOutputStream::DoDestroy()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    AtomicSet(IsWritable, 0);

    DisposeBuffers();

    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSOutputStream::IsEmpty(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSOutputStream* stream =
        ObjectWrap::Unwrap<TNodeJSOutputStream>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 0);

    // Do the work.
    return scope.Close(stream->DoIsEmpty());
}

Handle<Value> TNodeJSOutputStream::DoIsEmpty()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return scope.Close(Boolean::New(Queue.IsEmpty()));
}

////////////////////////////////////////////////////////////////////////////////

int TNodeJSOutputStream::AsyncOnWrite(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    TNodeJSOutputStream* stream = static_cast<TNodeJSOutputStream*>(request->data);
    AtomicSet(stream->WriteRequestPending, 0);
    stream->DoOnWrite();
    stream->AsyncUnref();
    return 0;
}

void TNodeJSOutputStream::DoOnWrite()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TOutputPart part;
    while (Queue.Dequeue(&part)) {
        node::Buffer* buffer =
            node::Buffer::New(part.Buffer, part.Length, DeleteCallback, NULL);

        Local<Value> args[] = {
            Local<Value>::New(buffer->handle_)
        };

        node::MakeCallback(this->handle_, OnWriteSymbol, ARRAY_SIZE(args), args);
    }

    node::MakeCallback(this->handle_, OnDrainSymbol, 0, NULL);
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::DoWrite(const void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    if (!AtomicGet(IsWritable) || data == NULL || length == 0) {
        return;
    }

    char* buffer = new char[length];
    YASSERT(buffer);

    ::memcpy(buffer, data, length);

    TOutputPart part;
    part.Buffer = buffer;
    part.Length = length;
    Queue.Enqueue(part);

    // We require that calling party holds a synchronous lock on the stream.
    // In case of TNodeJSDriver an instance TNodeJSInputStack holds a lock
    // and TNodeJSDriver implementation guarantees that all Write() calls
    // are within scope of the lock.
    
    EnqueueOnWrite();

    AtomicAdd(BytesCounter, length);
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::DisposeBuffers()
{
    TOutputPart part;
    while (Queue.Dequeue(&part)) {
        DeleteCallback(part.Buffer, NULL);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
