#include "input_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

Persistent<FunctionTemplate> TNodeJSInputStream::ConstructorTemplate;

////////////////////////////////////////////////////////////////////////////////

TNodeJSInputStream::TNodeJSInputStream()
    : TNodeJSStreamBase()
    , IsAlive(true)
{
    T_THREAD_AFFINITY_IS_V8();
}

TNodeJSInputStream::~TNodeJSInputStream() throw()
{
    T_THREAD_AFFINITY_IS_V8();

    {
        TGuard<TMutex> guard(&Mutex);
        YASSERT(Queue.empty());
    }
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSInputStream::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TNodeJSInputStream::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TNodeJSInputStream"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Push",  TNodeJSInputStream::Push );
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Sweep", TNodeJSInputStream::Sweep);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Close", TNodeJSInputStream::Close);

    target->Set(
        String::NewSymbol("TNodeJSInputStream"),
        ConstructorTemplate->GetFunction());
}

bool TNodeJSInputStream::HasInstance(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return
        value->IsObject() &&
        ConstructorTemplate->HasInstance(value->ToObject());
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
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSInputStream* stream =
        ObjectWrap::Unwrap<TNodeJSInputStream>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 3);

    EXPECT_THAT_HAS_INSTANCE(args[0], node::Buffer);
    EXPECT_THAT_IS(args[1], Uint32);
    EXPECT_THAT_IS(args[2], Uint32);

    // Do the work.
    return scope.Close(stream->DoPush(
        /* handle */ Persistent<Value>::New(args[0]),
        /* buffer */ node::Buffer::Data(args[0].As<Object>()),
        /* offset */ args[1]->Uint32Value(),
        /* length */ args[2]->Uint32Value()));
}

Handle<Value> TNodeJSInputStream::DoPush(Persistent<Value> handle, char* buffer, size_t offset, size_t length)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TInputPart* part = new TInputPart();
    YASSERT(part);

    part->Stream = this;
    part->Handle = handle;
    part->Buffer = buffer;
    part->Offset = offset;
    part->Length = length;

    {
        TGuard<TMutex> guard(&Mutex);
        Queue.push_back(part);
        Conditional.BroadCast();
    }

    // TODO(sandello): Think about OnSuccess & OnError callbacks.
    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TAlreadyLockedOps {
    static inline void Acquire(T* t)
    { }
    static inline void Release(T* t)
    {
        t->Release();
    }
};

Handle<Value> TNodeJSInputStream::Sweep(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSInputStream* stream =
        ObjectWrap::Unwrap<TNodeJSInputStream>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 0);

    // Do the work.
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
        if (!Mutex.TryAcquire()) {
            EnqueueSweep();
            return;
        }
    }

    TGuard< TMutex, TAlreadyLockedOps<TMutex> > guard(&Mutex);

    auto
        it = Queue.begin(),
        jt = Queue.end();

    while (it != jt) {
        TInputPart* part = *it;

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
    YASSERT(args.Length() == 0);

    // Do the work.
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

    TGuard<TMutex> guard(&Mutex);

    IsAlive = false;
    Conditional.BroadCast();
}

////////////////////////////////////////////////////////////////////////////////

size_t TNodeJSInputStream::DoRead(void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    TGuard<TMutex> guard(&Mutex);

    size_t result = 0;
    while (length > 0 && result == 0) {
        auto
            it = Queue.begin(),
            jt = Queue.end();

        size_t canRead;
        bool canReadSomething = false;

        while (length > 0 && it != jt) {
            TInputPart* part = *it;

            canRead = std::min(length, part->Length);
            canReadSomething |= (canRead > 0);

            ::memcpy(
                (char*)data + result,
                part->Buffer + part->Offset,
                canRead);

            result += canRead;
            length -= canRead;

            part->Offset += canRead;
            part->Length -= canRead;

            YASSERT(length == 0 || part->Length == 0);

            ++it;
        }

        if (!canReadSomething) {
            if (IsAlive) {
                Conditional.WaitI(Mutex);
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
