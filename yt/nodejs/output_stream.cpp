#include "output_stream.h"

#include <node.h>
#include <node_buffer.h>

namespace NYT {

COMMON_V8_USES

static Persistent<String> OnWriteSymbol;

////////////////////////////////////////////////////////////////////////////////

TNodeJSOutputStream::TNodeJSOutputStream()
{
    T_THREAD_AFFINITY_IS_V8();

    CHECK_RETURN_VALUE(pthread_mutex_init(&Mutex, NULL));
}

TNodeJSOutputStream::~TNodeJSOutputStream()
{
    // Affinity: any?
    TRACE_CURRENT_THREAD();

    CHECK_RETURN_VALUE(pthread_mutex_destroy(&Mutex));

    // TODO(sandello): Use FOREACH.
    TQueue::iterator
        it = Queue.begin(),
        jt = Queue.end();

    while (it != jt) {
        if (it->Data) {
            delete[] (char*)it->Data;
            it->Data = NULL;
        }
    }
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

Handle<Value> TNodeJSOutputStream::Pull(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSOutputStream* stream = 
        ObjectWrap::Unwrap<TNodeJSOutputStream>(args.This());

    // Validate arguments.
    assert(args.Length() == 0);

    // Do the work.
    assert(stream);
    return stream->DoPull();
}

Handle<Value> TNodeJSOutputStream::DoPull()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TGuard guard(&Mutex);

    if (Queue.empty()) {
        return Null();
    } else {
        TPart& front = Queue.front();

        node::Buffer* buffer =
            node::Buffer::New(front.Data, front.Length, DeleteCallback, 0);

        Queue.pop_front();
        return scope.Close(buffer->handle_);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::AsyncWriteCallback(uv_work_t* request)
{
    THREAD_AFFINITY_IS_V8();
    TNodeJSOutputStream* stream =
        container_of(request, TNodeJSOutputStream, WriteCallbackRequest);
    stream->DoWriteCallback();
}

void TNodeJSOutputStream::DoWriteCallback()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // TODO(sandello): Use OnWriteSymbol here.
    node::MakeCallback(handle_, "on_write", 0, NULL);
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSOutputStream::Write(const void *buffer, size_t length)
{
    // Affinity: any
    // TODO(sandello): Use some kind of holder here.
    char* data = new char[length]; assert(data);
    ::memcpy(data, buffer, length);

    TPart part;
    part.Data   = data;
    part.Offset = 0;
    part.Length = length;

    {
        TGuard guard(&Mutex);
        Queue.push_back(part);
    }

    EnqueueWriteCallback();
}

void TNodeJSOutputStream::DeleteCallback(char *data, void *hint)
{
    delete[] data;
}

////////////////////////////////////////////////////////////////////////////////

void ExportOutputStream(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    OnWriteSymbol = NODE_PSYMBOL("on_write");

    Local<FunctionTemplate> tpl = FunctionTemplate::New(TNodeJSOutputStream::New);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Pull", TNodeJSOutputStream::Pull);

    tpl->SetClassName(String::NewSymbol("TNodeJSOutputStream"));
    target->Set(String::NewSymbol("TNodeJSOutputStream"), tpl->GetFunction());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
