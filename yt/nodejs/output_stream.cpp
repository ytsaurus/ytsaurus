#include "output_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {
void DeleteCallback(char* data, void* hint)
{
    delete[] data;
}
} // namespace

COMMON_V8_USES

Persistent<FunctionTemplate> TNodeJSOutputStream::ConstructorTemplate;

static Persistent<String> OnWriteSymbol;
static Persistent<String> OnFlushSymbol;
static Persistent<String> OnFinishSymbol;

////////////////////////////////////////////////////////////////////////////////

TNodeJSOutputStream::TNodeJSOutputStream()
{
    T_THREAD_AFFINITY_IS_V8();
}

TNodeJSOutputStream::~TNodeJSOutputStream()
{
    T_THREAD_AFFINITY_IS_V8();
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    OnWriteSymbol  = NODE_PSYMBOL("on_write");
    OnFlushSymbol  = NODE_PSYMBOL("on_flush");
    OnFinishSymbol = NODE_PSYMBOL("on_Finish");

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
    T_THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TNodeJSOutputStream* stream = new TNodeJSOutputStream();
    stream->Wrap(args.This());
    return scope.Close(args.This());
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::AsyncOnWrite(uv_work_t* request)
{
    THREAD_AFFINITY_IS_V8();
    TPart* part = container_of(request, TPart, Request);
    TNodeJSOutputStream* stream = (TNodeJSOutputStream*)(part->Stream);
    stream->DoOnWrite(part);
}

void TNodeJSOutputStream::DoOnWrite(TPart* part)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    node::Buffer* buffer =
        node::Buffer::New(part->Data, part->Length, DeleteCallback, 0);
 
    Local<Value> args[] = { Local<Value>::New(buffer->handle_) };
    // TODO(sandello): Use OnWriteSymbol here.
    node::MakeCallback(this->handle_, "on_write", ARRAY_SIZE(args), args);

    delete part;
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

void TNodeJSOutputStream::Write(const void *buffer, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    char* data = new char[length]; assert(data);
    ::memcpy(data, buffer, length);

    TPart* part  = new TPart(); assert(part);
    part->Stream = this;
    part->Data   = data;
    part->Offset = 0;
    part->Length = length;

    EnqueueOnWrite(part);
}

void TNodeJSOutputStream::Flush()
{
    THREAD_AFFINITY_IS_ANY();
    EnqueueOnFlush();
}

void TNodeJSOutputStream::Finish()
{
    THREAD_AFFINITY_IS_ANY();
    EnqueueOnFinish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
