#include "input_stream.h"

#include <ytlib/misc/foreach.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

namespace {

void DoNothing()
{ }

static Persistent<String> OnDrainSymbol;
static Persistent<String> CurrentBufferSizeSymbol;
static Persistent<String> ActiveQueueSizeSymbol;
static Persistent<String> InactiveQueueSizeSymbol;

static const unsigned int NumberOfSpins = 4;

} // namespace

Persistent<FunctionTemplate> TNodeJSInputStream::ConstructorTemplate;

////////////////////////////////////////////////////////////////////////////////

TNodeJSInputStream::TNodeJSInputStream(ui64 lowWatermark, ui64 highWatermark)
    : TNodeJSStreamBase()
    , IsPushable(1)
    , IsReadable(1)
    , SweepRequestPending(0)
    , DrainRequestPending(0)
    , CurrentBufferSize(0)
    , LowWatermark(lowWatermark)
    , HighWatermark(highWatermark)
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

    OnDrainSymbol = NODE_PSYMBOL("on_drain");
    CurrentBufferSizeSymbol = NODE_PSYMBOL("current_buffer_size");
    ActiveQueueSizeSymbol = NODE_PSYMBOL("active_queue_size");
    InactiveQueueSizeSymbol = NODE_PSYMBOL("inactive_queue_size");

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

    YASSERT(args.Length() == 2);

    EXPECT_THAT_IS(args[0], Uint32);
    EXPECT_THAT_IS(args[1], Uint32);

    ui64 lowWatermark = args[0]->Uint32Value();
    ui64 highWatermark = args[1]->Uint32Value();

    TNodeJSInputStream* stream = NULL;
    try {
        char uuidBuffer[64];
        ::memset(uuidBuffer, 0, sizeof(uuidBuffer));

        stream = new TNodeJSInputStream(lowWatermark, highWatermark);
        stream->Wrap(args.This());
        stream->PrintUuid(uuidBuffer);

        stream->handle_->Set(
            String::New("uuid"),
            String::New(uuidBuffer),
            (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));
        stream->handle_->Set(
            String::New("low_watermark"),
            Integer::NewFromUnsigned(lowWatermark),
            (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));
        stream->handle_->Set(
            String::New("high_watermark"),
            Integer::NewFromUnsigned(highWatermark),
            (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));

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

    if (!AtomicGet(IsPushable)) {
        return v8::Undefined();
    }

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

    if (AtomicAdd(CurrentBufferSize, length) > HighWatermark) {
        return v8::True();
    } else {
        return v8::False();
    }
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
        AtomicSet(IsPushable, 0);
        AtomicBarrier();
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
        DisposeHandles(&InactiveQueue);
        DisposeHandles(&ActiveQueue);
        AtomicSet(IsPushable, 0);
        AtomicSet(IsReadable, 0);
        AtomicBarrier();
        Conditional.BroadCast();
    }

    EnqueueDrain();

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

int TNodeJSInputStream::AsyncSweep(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    TNodeJSInputStream* stream = static_cast<TNodeJSInputStream*>(request->data);
    AtomicSet(stream->SweepRequestPending, 0);
    stream->DoSweep();
    stream->AsyncUnref();
    return 0;
}

void TNodeJSInputStream::DoSweep()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Since this function is invoked from V8, we are trying to avoid
    // all blocking operations. For example, it is better to reschedule
    // the sweep if the mutex is already acquired.
    {
        bool mutexAcquired = false;
        for (unsigned int outerSpin = 0; outerSpin < NumberOfSpins; ++outerSpin) {
            if (Mutex.TryAcquire()) {
                mutexAcquired = true;
                break;
            }
            for (unsigned int innerSpin = 0; innerSpin < outerSpin * outerSpin; ++innerSpin) {
                DoNothing();
            }
        }

        if (!mutexAcquired) {
            EnqueueSweep();
            return;
        }
    }

    TGuard< TMutex, TAlreadyLockedOps<TMutex> > guard(&Mutex);
    DisposeHandles(&InactiveQueue);

    UpdateV8Properties();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TNodeJSInputStream::Drain(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TNodeJSInputStream* stream =
        ObjectWrap::Unwrap<TNodeJSInputStream>(args.This());

    // Validate arguments.
    YASSERT(args.Length() == 0);

    // Do the work.
    stream->EnqueueDrain();

    return Undefined();
}

int TNodeJSInputStream::AsyncDrain(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    TNodeJSInputStream* stream = static_cast<TNodeJSInputStream*>(request->data);
    AtomicSet(stream->DrainRequestPending, 0);
    stream->DoDrain();
    stream->AsyncUnref();
    return 0;
}

void TNodeJSInputStream::DoDrain()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    UpdateV8Properties();

    // TODO(sandello): Use OnDrainSymbol here.
    node::MakeCallback(this->handle_, "on_drain", 0, NULL);
}

////////////////////////////////////////////////////////////////////////////////

size_t TNodeJSInputStream::DoRead(void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    if (!AtomicGet(IsReadable)) {
        return 0;
    }

    TScopedRef<false> guardAsyncReference(this);
    TGuard<TMutex> guard(&Mutex);

    size_t result = 0;
    while (length > 0 && result == 0) {
        auto
            it = ActiveQueue.begin(),
            jt = ActiveQueue.end(),
            kt = ActiveQueue.begin();

        size_t canRead = 0;
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
                ++it;
                ++kt;
            } else {
                ++it;
            }
        }

        InactiveQueue.insert(InactiveQueue.end(), ActiveQueue.begin(), kt);
        ActiveQueue.erase(ActiveQueue.begin(), kt);

        if (!canReadSomething) {
            if (!AtomicGet(IsPushable)) {
                return 0;
            }

            Conditional.WaitI(Mutex);
            continue;
        }
    };

    if (AtomicSub(CurrentBufferSize, result) < LowWatermark) {
        EnqueueSweep();
        EnqueueDrain();
    } else {
        EnqueueSweep();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TNodeJSInputStream::UpdateV8Properties()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    handle_->Set(
        CurrentBufferSizeSymbol,
        Integer::NewFromUnsigned(AtomicGet(CurrentBufferSize)),
        (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));

    handle_->Set(
        ActiveQueueSizeSymbol,
        Integer::NewFromUnsigned(ActiveQueue.size()),
        (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));

    handle_->Set(
        InactiveQueueSizeSymbol,
        Integer::NewFromUnsigned(InactiveQueue.size()),
        (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));
}

void TNodeJSInputStream::DisposeHandles(std::deque<TInputPart*>* queue)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    FOREACH (auto* part, *queue) {
        part->Handle.Dispose();
        part->Handle.Clear();
        delete part;
    }

    queue->clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
