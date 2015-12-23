#include "input_stream.h"

#include <util/system/spinlock.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

////////////////////////////////////////////////////////////////////////////////

namespace {

static Persistent<String> OnDrainSymbol;
static Persistent<String> ActiveQueueSizeSymbol;
static Persistent<String> InactiveQueueSizeSymbol;

static const unsigned int NumberOfSpins = 4;

} // namespace

////////////////////////////////////////////////////////////////////////////////

Persistent<FunctionTemplate> TInputStreamWrap::ConstructorTemplate;

TInputStreamWrap::TInputStreamWrap(ui64 lowWatermark, ui64 highWatermark)
    : TNodeJSStreamBase()
    , IsPushable(1)
    , IsReadable(1)
    , SweepRequestPending(0)
    , DrainRequestPending(0)
    , BytesInFlight(0)
    , BytesEnqueued(0)
    , BytesDequeued(0)
    , LowWatermark(lowWatermark)
    , HighWatermark(highWatermark)
{
    THREAD_AFFINITY_IS_V8();

    YCHECK(LowWatermark < HighWatermark);
}

TInputStreamWrap::~TInputStreamWrap() throw()
{
    THREAD_AFFINITY_IS_V8();

    Dispose();

    YCHECK(ActiveQueue.size() == 0);
    YCHECK(InactiveQueue.size() == 0);
}

////////////////////////////////////////////////////////////////////////////////

void TInputStreamWrap::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    OnDrainSymbol = NODE_PSYMBOL("on_drain");
    ActiveQueueSizeSymbol = NODE_PSYMBOL("active_queue_size");
    InactiveQueueSizeSymbol = NODE_PSYMBOL("inactive_queue_size");

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TInputStreamWrap::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TInputStreamWrap"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Push", TInputStreamWrap::Push);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Sweep", TInputStreamWrap::Sweep);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "End", TInputStreamWrap::End);
    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Destroy", TInputStreamWrap::Destroy);

    target->Set(
        String::NewSymbol("TInputStreamWrap"),
        ConstructorTemplate->GetFunction());
}

bool TInputStreamWrap::HasInstance(Handle<Value> value)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    return
        value->IsObject() &&
        ConstructorTemplate->HasInstance(value->ToObject());
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamWrap::New(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    YCHECK(args.Length() == 2);

    EXPECT_THAT_IS(args[0], Uint32);
    EXPECT_THAT_IS(args[1], Uint32);

    ui64 lowWatermark = args[0]->Uint32Value();
    ui64 highWatermark = args[1]->Uint32Value();

    TInputStreamWrap* stream = NULL;
    try {
        stream = new TInputStreamWrap(lowWatermark, highWatermark);
        stream->Wrap(args.This());

        stream->handle_->Set(
            String::NewSymbol("low_watermark"),
            Integer::NewFromUnsigned(lowWatermark),
            (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));
        stream->handle_->Set(
            String::NewSymbol("high_watermark"),
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

Handle<Value> TInputStreamWrap::Push(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TInputStreamWrap* stream =
        ObjectWrap::Unwrap<TInputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 3);

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

Handle<Value> TInputStreamWrap::DoPush(Persistent<Value> handle, char* buffer, size_t offset, size_t length)
{
    THREAD_AFFINITY_IS_V8();

    if (!AtomicGet(IsPushable)) {
        return Undefined();
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

    AtomicAdd(BytesEnqueued, length);
    auto transientSize = AtomicAdd(BytesInFlight, length);
    if (transientSize > HighWatermark) {
        return v8::False();
    } else {
        return v8::True();
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamWrap::End(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TInputStreamWrap* stream =
        ObjectWrap::Unwrap<TInputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    stream->DoEnd();

    return Undefined();
}

void TInputStreamWrap::DoEnd()
{
    THREAD_AFFINITY_IS_V8();

    {
        TGuard<TMutex> guard(&Mutex);
        AtomicSet(IsPushable, 0);
        AtomicBarrier();
        Conditional.BroadCast();
    }

    EnqueueSweep(true);
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamWrap::Destroy(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TInputStreamWrap* stream =
        ObjectWrap::Unwrap<TInputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    stream->DoDestroy();

    return Undefined();
}

void TInputStreamWrap::DoDestroy()
{
    THREAD_AFFINITY_IS_V8();

    Dispose();
    EnqueueDrain(true);
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

Handle<Value> TInputStreamWrap::Sweep(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TInputStreamWrap* stream =
        ObjectWrap::Unwrap<TInputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    stream->EnqueueSweep(true);

    return Undefined();
}

int TInputStreamWrap::AsyncSweep(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    TInputStreamWrap* stream = static_cast<TInputStreamWrap*>(request->data);
    AtomicSet(stream->SweepRequestPending, 0);
    stream->DoSweep();
    stream->AsyncUnref();
    return 0;
}

void TInputStreamWrap::DoSweep()
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
                SpinLockPause();
            }
        }

        if (!mutexAcquired) {
            EnqueueSweep(true);
            return;
        }
    }

    TGuard< TMutex, TAlreadyLockedOps<TMutex> > guard(&Mutex);
    DisposeHandles(&InactiveQueue);

    UpdateV8Properties();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamWrap::Drain(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    TInputStreamWrap* stream =
        ObjectWrap::Unwrap<TInputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    stream->EnqueueDrain(true);

    return Undefined();
}

int TInputStreamWrap::AsyncDrain(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    TInputStreamWrap* stream = static_cast<TInputStreamWrap*>(request->data);
    AtomicSet(stream->DrainRequestPending, 0);
    stream->DoDrain();
    stream->AsyncUnref();
    return 0;
}

void TInputStreamWrap::DoDrain()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    UpdateV8Properties();

    node::MakeCallback(this->handle_, OnDrainSymbol, 0, NULL);
}

////////////////////////////////////////////////////////////////////////////////

size_t TInputStreamWrap::DoRead(void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    if (!AtomicGet(IsReadable)) {
        THROW_ERROR_EXCEPTION("TInputStreamWrap was terminated");
    }

    TScopedRef<false> guardAsyncRef(this);
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

            if (!AtomicGet(IsReadable)) {
                THROW_ERROR_EXCEPTION("TInputStreamWrap was terminated");
            }

            continue;
        }
    };

    // (A note on Enqueue*() functions below.)
    // We require that calling party holds a synchronous lock on the stream.
    // In case of TDriverWrap an instance TNodeJSInputStack holds a lock
    // and TDriverWrap implementation guarantees that all Read() calls
    // are within scope of the lock.

    if (!InactiveQueue.empty()) {
        EnqueueSweep(false);
    }

    AtomicAdd(BytesDequeued, result);
    auto transientSize = AtomicSub(BytesInFlight, result);
    if (transientSize < LowWatermark && LowWatermark <= transientSize + result) {
        EnqueueDrain(false);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TInputStreamWrap::UpdateV8Properties()
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    handle_->Set(
        ActiveQueueSizeSymbol,
        Integer::NewFromUnsigned(ActiveQueue.size()),
        (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));

    handle_->Set(
        InactiveQueueSizeSymbol,
        Integer::NewFromUnsigned(InactiveQueue.size()),
        (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));
}

void TInputStreamWrap::Dispose()
{
    THREAD_AFFINITY_IS_V8();

    TGuard<TMutex> guard(&Mutex);
    DisposeHandles(&InactiveQueue);
    DisposeHandles(&ActiveQueue);
    AtomicSet(IsPushable, 0);
    AtomicSet(IsReadable, 0);
    AtomicBarrier();
    Conditional.BroadCast();
}

void TInputStreamWrap::DisposeHandles(std::deque<TInputPart*>* queue)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    for (auto* part : *queue) {
        part->Handle.Dispose();
        part->Handle.Clear();
        delete part;
    }

    queue->clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT

