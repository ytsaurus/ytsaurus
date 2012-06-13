#include "output_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

namespace {

static Persistent<String> OnWriteSymbol;
static Persistent<String> OnDrainSymbol;
static Persistent<String> OnFlushSymbol;
static Persistent<String> OnFinishSymbol;

void DeleteCallback(char* data, void* hint)
{
    delete[] data;
}

} // namespace

Persistent<FunctionTemplate> TNodeJSOutputStream::ConstructorTemplate;

////////////////////////////////////////////////////////////////////////////////


TNodeJSOutputStream::TNodeJSOutputStream()
    : TNodeJSStreamBase()
    , IsAlive(1)
{
    THREAD_AFFINITY_IS_V8();
}

TNodeJSOutputStream::~TNodeJSOutputStream() throw()
{
    THREAD_AFFINITY_IS_V8();

    // Diagnostics about deleting with non-empty queue.
    TOutputPart part;
    while (Queue.Dequeue(&part)) {
        DeleteCallback(part.Buffer, NULL);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    OnWriteSymbol  = NODE_PSYMBOL("on_write");
    OnDrainSymbol  = NODE_PSYMBOL("on_drain");
    OnFlushSymbol  = NODE_PSYMBOL("on_flush");
    OnFinishSymbol = NODE_PSYMBOL("on_finish");

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

    NDetail::AtomicallyStore(&IsAlive, 0);

    TOutputPart part;
    while (Queue.Dequeue(&part)) {
        DeleteCallback(part.Buffer, NULL);
    }

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

void TNodeJSOutputStream::AsyncOnWrite(uv_work_t* request)
{
    THREAD_AFFINITY_IS_V8();
    TNodeJSOutputStream* stream =
        container_of(request, TNodeJSOutputStream, WriteRequest);
    stream->DoOnWrite();
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

        // TODO(sandello): Use OnWriteSymbol here.
        node::MakeCallback(this->handle_, "on_write", ARRAY_SIZE(args), args);
    }

    // TODO(sandello): Use OnDrainSymbol here.
    node::MakeCallback(this->handle_, "on_drain", 0, NULL);

    AsyncUnref();
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::AsyncOnFlush(uv_work_t* request)
{
    THREAD_AFFINITY_IS_V8();
    TNodeJSOutputStream* stream =
        container_of(request, TNodeJSOutputStream, FlushRequest);
    stream->DoOnFlush();
}

void TNodeJSOutputStream::DoOnFlush()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // TODO(sandello): Use OnFlushSymbol here.
    node::MakeCallback(this->handle_, "on_flush", 0, NULL);

    AsyncUnref();
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::AsyncOnFinish(uv_work_t* request)
{
    THREAD_AFFINITY_IS_V8();
    TNodeJSOutputStream* stream =
        container_of(request, TNodeJSOutputStream, FinishRequest);
    stream->DoOnFinish();
}

void TNodeJSOutputStream::DoOnFinish()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // TODO(sandello): Use OnFinishSymbol here.
    node::MakeCallback(this->handle_, "on_finish", 0, NULL);

    AsyncUnref();
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::DoWrite(const void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    if (length == 0) {
        return;
    }

    if (!NDetail::AtomicallyFetch(&IsAlive)) {
        return;
    }

    char* buffer = new char[length];
    YASSERT(buffer);

    ::memcpy(buffer, data, length);

    TOutputPart part;
    part.Buffer = buffer;
    part.Length = length;
    Queue.Enqueue(part);

    EnqueueOnWrite();
}

void TNodeJSOutputStream::DoFlush()
{
    THREAD_AFFINITY_IS_ANY();
    EnqueueOnFlush();
}

void TNodeJSOutputStream::DoFinish()
{
    THREAD_AFFINITY_IS_ANY();
    EnqueueOnFinish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
