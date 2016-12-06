#include "input_stream.h"

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

COMMON_V8_USES

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

static Persistent<String> OnDrainSymbol;

static const unsigned int NumberOfSpins = 4;

} // namespace


////////////////////////////////////////////////////////////////////////////////

Persistent<FunctionTemplate> TInputStreamWrap::ConstructorTemplate;

TInputStreamWrap::TInputStreamWrap(ui64 lowWatermark, ui64 highWatermark)
    : TNodeJSStreamBase()
    , LowWatermark_(lowWatermark)
    , HighWatermark_(highWatermark)
{
    THREAD_AFFINITY_IS_V8();

    YCHECK(LowWatermark_ < HighWatermark_);
}

TInputStreamWrap::~TInputStreamWrap() throw()
{
    THREAD_AFFINITY_IS_V8();

    DisposeStream();

    YCHECK(ActiveQueue_.size() == 0);
    YCHECK(InactiveQueue_.size() == 0);
}

////////////////////////////////////////////////////////////////////////////////

void TInputStreamWrap::Initialize(Handle<Object> target)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    OnDrainSymbol = NODE_PSYMBOL("on_drain");

    ConstructorTemplate = Persistent<FunctionTemplate>::New(
        FunctionTemplate::New(TInputStreamWrap::New));

    ConstructorTemplate->InstanceTemplate()->SetInternalFieldCount(1);
    ConstructorTemplate->SetClassName(String::NewSymbol("TInputStreamWrap"));

    NODE_SET_PROTOTYPE_METHOD(ConstructorTemplate, "Push", TInputStreamWrap::Push);
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

    try {
        ui64 lowWatermark = args[0]->Uint32Value();
        ui64 highWatermark = args[1]->Uint32Value();

        auto* stream = new TInputStreamWrap(lowWatermark, highWatermark);
        stream->Wrap(args.This());

        stream->handle_->Set(
            String::NewSymbol("low_watermark"),
            Integer::NewFromUnsigned(lowWatermark),
            (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));
        stream->handle_->Set(
            String::NewSymbol("high_watermark"),
            Integer::NewFromUnsigned(highWatermark),
            (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));
        stream->handle_->Set(
            String::NewSymbol("cxx_id"),
            Integer::NewFromUnsigned(stream->Id_),
            (v8::PropertyAttribute)(v8::ReadOnly | v8::DontDelete));

        return scope.Close(args.This());
    } catch (const std::exception& ex) {
        return ThrowException(Exception::Error(String::New(ex.what())));
    }
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamWrap::Push(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* stream = ObjectWrap::Unwrap<TInputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 3);

    EXPECT_THAT_HAS_INSTANCE(args[0], node::Buffer);
    EXPECT_THAT_IS(args[1], Uint32);
    EXPECT_THAT_IS(args[2], Uint32);

    auto handle = Persistent<Value>::New(args[0]);
    auto buffer = node::Buffer::Data(args[0].As<Object>());
    auto offset = args[1]->Uint32Value();
    auto length = args[2]->Uint32Value();

    // Do the work.
    auto canPushAgain = stream->DoPush(handle, buffer, offset, length);
    return scope.Close(Boolean::New(canPushAgain));
}

bool TInputStreamWrap::DoPush(Persistent<Value> handle, char* buffer, size_t offset, size_t length)
{
    THREAD_AFFINITY_IS_V8();

    bool canPushAgain = false;

    ProtectedUpdateAndNotifyReader([&] () {
        if (!IsPushable_) {
            return;
        }

        auto part = std::make_unique<TInputPart>();
        part->Handle = handle;
        part->Buffer = buffer;
        part->Offset = offset;
        part->Length = length;

        ActiveQueue_.emplace_back(std::move(part));

        BytesEnqueued_ += length;
        BytesInFlight_ += length;

        canPushAgain = BytesInFlight_ < HighWatermark_;
    });

    return canPushAgain;
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamWrap::End(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* stream = ObjectWrap::Unwrap<TInputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    stream->DoEnd();

    return scope.Close(Undefined());
}

void TInputStreamWrap::DoEnd()
{
    THREAD_AFFINITY_IS_V8();

    ProtectedUpdateAndNotifyReader([&] () {
        IsPushable_ = false;
    });

    EnqueueSweep();
}

////////////////////////////////////////////////////////////////////////////////

Handle<Value> TInputStreamWrap::Destroy(const Arguments& args)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    // Unwrap.
    auto* stream = ObjectWrap::Unwrap<TInputStreamWrap>(args.This());

    // Validate arguments.
    YCHECK(args.Length() == 0);

    // Do the work.
    stream->DoDestroy();

    return scope.Close(Undefined());
}

void TInputStreamWrap::DoDestroy()
{
    THREAD_AFFINITY_IS_V8();

    DisposeStream();

    EnqueueDrain();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TSpinningLockOps
{
    // When a function is invoked from V8, we are trying to avoid
    // all blocking operations. So it is better to reschedule the function
    // if the mutex is already acquired.
    static inline bool TryAcquire(T* t)
    {
        bool mutexAcquired = false;
        for (unsigned int outerSpin = 0; outerSpin < NumberOfSpins; ++outerSpin) {
            if (t->TryAcquire()) {
                mutexAcquired = true;
                break;
            }
            for (unsigned int innerSpin = 0; innerSpin < outerSpin * outerSpin; ++innerSpin) {
                SpinLockPause();
            }
        }
        return mutexAcquired;
    }

    static inline void Release(T* t)
    {
        t->Release();
    }
};

int TInputStreamWrap::AsyncSweep(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    auto* stream = static_cast<TInputStreamWrap*>(request->data);
    stream->SweepRequestPending_.store(false, std::memory_order_release);
    stream->DoSweep();
    stream->AsyncUnref();

    return 0;
}

void TInputStreamWrap::EnqueueSweep()
{
    bool expected = false;
    if (SweepRequestPending_.compare_exchange_strong(expected, true, std::memory_order_acquire)) {
        AsyncRef();
        EIO_PUSH(TInputStreamWrap::AsyncSweep, this);
    }
}

void TInputStreamWrap::DoSweep()
{
    THREAD_AFFINITY_IS_V8();

    {
        TTryGuard<TMutex, TSpinningLockOps<TMutex>> guard(&Mutex_);
        if (!guard) {
            EnqueueSweep();
            return;
        }

        DisposeHandles(&InactiveQueue_);
    }
}

////////////////////////////////////////////////////////////////////////////////

int TInputStreamWrap::AsyncDrain(eio_req* request)
{
    THREAD_AFFINITY_IS_V8();
    HandleScope scope;

    auto* stream = static_cast<TInputStreamWrap*>(request->data);
    stream->DrainRequestPending_.store(false, std::memory_order_release);
    node::MakeCallback(stream->handle_, OnDrainSymbol, 0, nullptr);
    stream->AsyncUnref();

    return 0;
}

void TInputStreamWrap::EnqueueDrain()
{
    bool expected = false;
    if (DrainRequestPending_.compare_exchange_strong(expected, true, std::memory_order_acquire)) {
        AsyncRef();
        EIO_PUSH(TInputStreamWrap::AsyncDrain, this);
    }
}

////////////////////////////////////////////////////////////////////////////////

const ui64 TInputStreamWrap::GetBytesEnqueued() const
{
    auto guard = Guard(Mutex_);
    return BytesEnqueued_;
}

const ui64 TInputStreamWrap::GetBytesDequeued() const
{
    auto guard = Guard(Mutex_);
    return BytesDequeued_;
}

size_t TInputStreamWrap::DoRead(void* data, size_t length)
{
    THREAD_AFFINITY_IS_ANY();

    TIntrusivePtr<IAsyncRefCounted> ref(this);
    TGuard<TMutex> guard(&Mutex_);

    size_t result = 0;
    while (length > 0 && result == 0) {
        if (!IsReadable_) {
            THROW_ERROR_EXCEPTION("TInputStreamWrap was terminated");
        }

        auto
            it = ActiveQueue_.begin(),
            jt = ActiveQueue_.end(),
            kt = ActiveQueue_.begin();

        size_t canRead = 0;
        bool canReadSomething = false;

        while (length > 0 && it != jt) {
            const std::unique_ptr<TInputPart>& part = *it;

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

            Y_ASSERT(length == 0 || part->Length == 0);

            if (part->Length == 0) {
                Y_ASSERT(it == kt);
                ++it;
                ++kt;
            } else {
                ++it;
            }
        }

        InactiveQueue_.insert(
            InactiveQueue_.end(),
            std::make_move_iterator(ActiveQueue_.begin()),
            std::make_move_iterator(kt));
        ActiveQueue_.erase(ActiveQueue_.begin(), kt);

        if (!canReadSomething) {
            if (!IsPushable_) {
                length = 0;
                break;
            }

            YCHECK(!ReadPromise_);
            ReadPromise_ = NewPromise<void>();

            auto readPromise = ReadPromise_;

            {
                auto unguard = Unguard(Mutex_);
                WaitFor(readPromise.ToFuture())
                    .ThrowOnError();
            }
        }
    };

    // (A note on Enqueue*() functions below.)
    // We require that calling party holds a synchronous lock on the stream.
    // In case of TDriverWrap an instance TNodeJSInputStack holds a lock
    // and TDriverWrap implementation guarantees that all Read() calls
    // are within scope of the lock.

    if (!InactiveQueue_.empty()) {
        EnqueueSweep();
    }

    BytesDequeued_ += result;
    BytesInFlight_ -= result;

    if (BytesInFlight_ < LowWatermark_ && LowWatermark_ <= BytesInFlight_ + result) {
        EnqueueDrain();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TInputStreamWrap::ProtectedUpdateAndNotifyReader(std::function<void()> mutator)
{
    TPromise<void> readPromise;
    {
        auto guard = Guard(Mutex_);
        mutator();
        if (ReadPromise_) {
            readPromise = std::move(ReadPromise_);
        }
    }
    if (readPromise) {
        readPromise.Set();
    }
}

void TInputStreamWrap::DisposeStream()
{
    THREAD_AFFINITY_IS_V8();

    ProtectedUpdateAndNotifyReader([&] () {
        IsPushable_ = false;
        IsReadable_ = false;
        DisposeHandles(&InactiveQueue_);
        DisposeHandles(&ActiveQueue_);
    });
}

void TInputStreamWrap::DisposeHandles(std::deque<std::unique_ptr<TInputPart>>* queue)
{
    THREAD_AFFINITY_IS_V8();

    for (auto& part : *queue) {
        part->Handle.Dispose();
        part->Handle.Clear();
        part.reset();
    }

    queue->clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT

