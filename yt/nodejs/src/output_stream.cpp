#include "output_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

namespace {

static Persistent<String> OnWriteSymbol;
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
{
    THREAD_AFFINITY_IS_V8();
}

TNodeJSOutputStream::~TNodeJSOutputStream() throw()
{
    THREAD_AFFINITY_IS_V8();

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
    OnFlushSymbol  = NODE_PSYMBOL("on_flush");
    OnFinishSymbol = NODE_PSYMBOL("on_finish");

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TNodeJSOutputStream::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TNodeJSOutputStream"));

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
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::DoWrite(const void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    if (length == 0) {
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
