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
    THREAD_AFFINITY_IS_V8();

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
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TNodeJSInputStream* stream = new TNodeJSInputStream();
    stream->Wrap(args.This());
    return scope.Close(args.This());
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSInputStream::Push(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
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
    return stream->DoPush(
        /* buffer */ args[0],
        /* data   */ node::Buffer::Data(Local<Object>::Cast(args[0])),
        /* offset */ args[1]->Uint32Value(),
        /* length */ args[2]->Uint32Value());
}

Handle<Value> TNodeJSInputStream::DoPush(Handle<Value> buffer, char *data, size_t offset, size_t length)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TPart* part  = new TPart(); assert(part);
    part->Stream = this;
    part->Handle = handle;
    part->Data   = data;
    part->Offset = offset;
    part->Length = length;

    {
        TGuard guard(&Mutex);
        Queue.push_back(part);
        pthread_cond_broadcast(&Conditional);
    }

    // TODO(sandello): Think about OnSuccess & OnError callbacks.
    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSInputStream::Sweep(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
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


void TNodeJSInputStream::AsyncSweep(uv_work_t* request)
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
        TPart* part = *it;

        if (part->Length > 0) {
            break;
        } else {
            part->Handle.Dispose();
            part->Handle.Clear();
            delete part;

            ++it;
        }
    }

    Queue.erase(Queue.begin(), it);
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSInputStream::Close(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
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

void TNodeJSInputStream::AsyncClose(uv_work_t* request)
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

////////////////////////////////////////////////////////////////////////////////

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
            TPart* part = *it;

            canRead = std::min(length, part->Length);
            canReadSomething |= (canRead > 0);

            ::memcpy(
                (char*)buffer + result,
                part->Data + part->Offset,
                canRead);

            result += canRead;
            length -= canRead;

            part->Offset += canRead;
            part->Length -= canRead;

            assert(length == 0 || part->Length == 0);

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

void ExportInputStream(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    Local<FunctionTemplate> tpl = FunctionTemplate::New(TNodeJSInputStream::New);

    tpl->InstanceTemplate()->SetInternalFieldCount(1);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Push", TNodeJSInputStream::Push);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Sweep", TNodeJSInputStream::Sweep);
    NODE_SET_PROTOTYPE_METHOD(tpl, "Close", TNodeJSInputStream::Close);

    tpl->SetClassName(String::NewSymbol("TNodeJSInputStream"));
    target->Set(String::NewSymbol("TNodeJSInputStream"), tpl->GetFunction());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
