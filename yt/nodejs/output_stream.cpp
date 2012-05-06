#include "output_stream.h"

#include <node.h>
#include <node_buffer.h>

namespace NYT {

COMMON_V8_USES

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
/*

TODO

- implement a test callback , which writes some data to output stream
- pull data from JS and test in that way

*/

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
}

void TNodeJSOutputStream::DeleteCallback(char *data, void *hint)
{
    delete[] data;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
