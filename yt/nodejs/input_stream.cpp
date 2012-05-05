#include "input_stream.h"

#include <node.h>
#include <node_buffer.h>

namespace NYT {

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

TNodeJSInputStream::TNodeJSInputStream()
    : TNodeJSStreamBase()
    , IsAlive(false)
{
    T_THREAD_AFFINITY_IS_V8();

    CHECK_RETURN_VALUE(pthread_mutex_init(&Mutex, NULL));
    CHECK_RETURN_VALUE(pthread_cond_init(&Conditional, NULL));

    IsAlive = true;
}

TNodeJSInputStream::~TNodeJSInputStream()
{
    // Affinity: any?
    TRACE_CURRENT_THREAD("??");

    {
        TGuard guard(&Mutex);
        assert(Queue.empty());
    }

    CHECK_RETURN_VALUE(pthread_mutex_destroy(&Mutex));
    CHECK_RETURN_VALUE(pthread_cond_destroy(&Conditional));
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSInputStream::New(const Arguments& args)
{
    T_THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TNodeJSInputStream* stream = new TNodeJSInputStream();
    stream->Wrap(args.This());
    return scope.Close(args.This());
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSInputStream::Push(const Arguments& args)
{
    T_THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSInputStream* stream =
        ObjectWrap::Unwrap<TNodeJSInputStream>(args.This());

    // Validate arguments.
    assert(args.Length() == 3);

    assert(node::Buffer::HasInstance(args[0]));
    assert(args[1]->IsUint32());
    assert(args[2]->IsUint32());

    // Do the work.
    assert(stream);
    stream->DoPush(
        /* buffer */ args[0],
        /* data   */ node::Buffer::Data(Local<Object>::Cast(args[0])),
        /* offset */ args[1]->Uint32Value(),
        /* length */ args[2]->Uint32Value());

    // TODO(sandello): Think about OnSuccess & OnError callbacks.
    return Undefined();
}

void TNodeJSInputStream::DoPush(Handle<Value> buffer, char *data, size_t offset, size_t length)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TPart part;
    part.Buffer = Persistent<Value>::New(buffer);
    part.Data   = data;
    part.Offset = offset;
    part.Length = length;

    {
        TGuard guard(&Mutex);
        Queue.push_back(part);
        pthread_cond_broadcast(&Conditional);
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSInputStream::Sweep(const Arguments& args)
{
    T_THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSInputStream* stream =
        ObjectWrap::Unwrap<TNodeJSInputStream>(args.This());

    // Validate arguments.
    assert(args.Length() == 0);

    // Do the work.
    assert(stream);
    stream->EnqueueSweep();

    // TODO(sandello): Think about OnSuccess & OnError callbacks.
    return Undefined();
}


void TNodeJSInputStream::AsyncSweep(uv_work_t *request)
{
    THREAD_AFFINITY_IS_V8();
    TNodeJSInputStream* stream =
        container_of(request, TNodeJSInputStream, SweepRequest);
    stream->DoSweep();
}

void TNodeJSInputStream::DoSweep()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Since this function is invoked from V8, we are trying to avoid
    // all blocking operations. For example, it is better to reschedule
    // the sweep, if the mutex is already acquired.
    {
        int rv  = pthread_mutex_trylock(&Mutex);
        if (rv != 0) {
            EnqueueSweep();
            return;
        }
    }

    TGuard guard(&Mutex, false);

    TQueue::iterator
        it = Queue.begin(),
        jt = Queue.end();

    while (it != jt) {
        TPart& current = *it;

        if (current.Length > 0) {
            break;
        } else {
            current.Buffer.Dispose();
            current.Buffer.Clear();
            ++it;
        }
    }

    Queue.erase(Queue.begin(), it);
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSInputStream::Close(const Arguments& args)
{
    T_THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSInputStream* stream =
        ObjectWrap::Unwrap<TNodeJSInputStream>(args.This());

    // Validate arguments.
    assert(args.Length() == 0);

    // Do the work.
    assert(stream);
    stream->EnqueueClose();

    return Undefined();
}

void TNodeJSInputStream::AsyncClose(uv_work_t *request)
{
    THREAD_AFFINITY_IS_UV();
    TNodeJSInputStream* stream =
        container_of(request, TNodeJSInputStream, CloseRequest);
    stream->DoClose();
}

void TNodeJSInputStream::DoClose()
{
    THREAD_AFFINITY_IS_UV();

    TGuard guard(&Mutex);

    IsAlive = false;
    pthread_cond_broadcast(&Conditional);
}

size_t TNodeJSInputStream::Read(void* buffer, size_t length)
{
    THREAD_AFFINITY_IS_UV();

    TGuard guard(&Mutex);

    size_t result = 0;
    while (length > 0 && result == 0) {
        TQueue::iterator
            it = Queue.begin(),
            jt = Queue.end();

        size_t canRead;
        bool canReadSomething = false;

        while (length > 0 && it != jt) {
            TPart& current = *it;

            canRead = std::min(length, current.Length);
            canReadSomething |= (canRead > 0);

            ::memcpy(
                (char*)buffer + result,
                current.Data + current.Offset,
                canRead);

            result += canRead;
            length -= canRead;

            current.Offset += canRead;
            current.Length -= canRead;

            assert(length == 0 || current.Length == 0);

            ++it;
        }

        if (!canReadSomething) {
            if (IsAlive) {
                CHECK_RETURN_VALUE(pthread_cond_wait(&Conditional, &Mutex));
                continue;
            } else {
                return 0;
            }
        }
    };

    EnqueueSweep();

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
