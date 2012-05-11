#include "output_stream.h"

#include <node.h>
#include <node_buffer.h>

namespace NYT {

namespace {
void DeleteCallback(char* data, void* hint)
{
    delete[] data;
}
} // namespace

COMMON_V8_USES

static Persistent<String> OnWriteSymbol;

////////////////////////////////////////////////////////////////////////////////

TNodeJSOutputStream::TNodeJSOutputStream()
{
    T_THREAD_AFFINITY_IS_V8();
}

TNodeJSOutputStream::~TNodeJSOutputStream()
{
    // Affinity: any?
    TRACE_CURRENT_THREAD("??");
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

////////////////////////////////////////////////////////////////////////////////

void ExportOutputStream(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    OnWriteSymbol = NODE_PSYMBOL("on_write");

    Local<FunctionTemplate> tpl = FunctionTemplate::New(TNodeJSOutputStream::New);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);
    tpl->SetClassName(String::NewSymbol("TNodeJSOutputStream"));

    target->Set(String::NewSymbol("TNodeJSOutputStream"), tpl->GetFunction());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
