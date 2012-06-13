#include "input_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

Persistent<FunctionTemplate> TNodeJSInputStream::ConstructorTemplate;

////////////////////////////////////////////////////////////////////////////////

TNodeJSInputStream::TNodeJSInputStream()
    : TNodeJSStreamBase()
    , IsAlive(1)
{
    THREAD_AFFINITY_IS_V8();
}

TNodeJSInputStream::~TNodeJSInputStream() throw()
{
    THREAD_AFFINITY_IS_V8();
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

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Push", TNodeJSInputStream::Push);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Sweep", TNodeJSInputStream::Sweep);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "End", TNodeJSInputStream::End);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Destroy", TNodeJSInputStream::Destroy);

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
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    TNodeJSInputStream* stream = NULL;
    try {
        stream = new TNodeJSInputStream();
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
        ActiveQueue.push_back(part);
        Conditional.BroadCast();
    }

    // TODO(sandello): Think about OnSuccess & OnError callbacks.
    return Undefined();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSInputStream::End(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSInputStream* stream =
        ObjectWrap::Unwrap<TNodeJSInputStream>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 0);

    // Do the work.
    return scope.Close(stream->DoEnd());
}

Handle<Value> TNodeJSInputStream::DoEnd()
{
    THREAD_AFFINITY_IS_V8();

    {
        TGuard<TMutex> guard(&Mutex);
        NDetail::AtomicallyStore(&IsAlive, 0);
        Conditional.BroadCast();
    }

    EnqueueSweep();

    return Undefined();
}


////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSInputStream::Destroy(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSInputStream* stream =
        ObjectWrap::Unwrap<TNodeJSInputStream>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 0);

    // Do the work.
    return scope.Close(stream->DoDestroy());
}

Handle<Value> TNodeJSInputStream::DoDestroy()
{
    THREAD_AFFINITY_IS_V8();

    {
        TGuard<TMutex> guard(&Mutex);

        InactiveQueue.insert(InactiveQueue.end(), ActiveQueue.begin(), ActiveQueue.end());
        ActiveQueue.clear();

        NDetail::AtomicallyStore(&IsAlive, 0);
        Conditional.BroadCast();
    }

    EnqueueSweep();

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

            AsyncUnref();

            return;
        }
    }

    TGuard< TMutex, TAlreadyLockedOps<TMutex> > guard(&Mutex);

    auto
        it = InactiveQueue.begin(),
        jt = InactiveQueue.end();

    while (it != jt) {
        TInputPart* part = *it;

        YASSERT(part->Length == 0);

        part->Handle.Dispose();
        part->Handle.Clear();

        delete part;

        ++it;
    }

    InactiveQueue.clear();

    AsyncUnref();
}

////////////////////////////////////////////////////////////////////////////////

size_t TNodeJSInputStream::DoRead(void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    TGuard<TMutex> guard(&Mutex);

    size_t result = 0;
    while (length > 0 && result == 0) {
        auto
            it = ActiveQueue.begin(),
            jt = ActiveQueue.end(),
            kt = ActiveQueue.begin();

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

            if (part->Length == 0) {
                YASSERT(it == kt);
                ++kt;
            }

            ++it;
        }

        InactiveQueue.insert(InactiveQueue.end(), ActiveQueue.begin(), kt);
        ActiveQueue.erase(ActiveQueue.begin(), kt);

        if (!canReadSomething) {
            if (!NDetail::AtomicallyFetch(&IsAlive)) {
                return 0;
            }

            Conditional.WaitI(Mutex);
            continue;
        }
    };

    EnqueueSweep();

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
