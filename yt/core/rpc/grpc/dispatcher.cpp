#include "dispatcher.h"
#include "helpers.h"

#include <yt/core/misc/shutdown.h>

#include <yt/core/concurrency/count_down_latch.h>

#include <util/system/thread.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>

#include <atomic>

namespace NYT::NRpc::NGrpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr int ThreadCount = 4;
static const auto& Logger = GrpcLogger;

////////////////////////////////////////////////////////////////////////////////

void* TCompletionQueueTag::GetTag(int cookie)
{
    Y_ASSERT(cookie >= 0 && cookie < 8);
    return reinterpret_cast<void*>(reinterpret_cast<intptr_t>(this) | cookie);
}

////////////////////////////////////////////////////////////////////////////////

static std::atomic<int> GrpcLibraryRefCounter;
static std::atomic<int> GrpcLibraryInitCounter;

TGrpcLibraryLock::TGrpcLibraryLock()
{
    if (GrpcLibraryRefCounter.fetch_add(1) == 0) {
        // Failure here indicates an attempt to re-initialize GRPC after shutdown.
        YCHECK(GrpcLibraryInitCounter.fetch_add(1) == 0);
        LOG_INFO("Initializing GRPC library");
        grpc_init_openssl();
        grpc_init();
    }
}

TGrpcLibraryLock::~TGrpcLibraryLock()
{
    if (GrpcLibraryRefCounter.fetch_sub(1) == 1) {
        LOG_INFO("Shutting down GRPC library");
        grpc_shutdown();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    TImpl()
        : Threads_(ThreadCount)
        , StartLatch_(ThreadCount)
        , LibraryLock_(CreateLibraryLock())
    {
        for (int index = 0; index < ThreadCount; ++index) {
            Threads_[index] = std::make_unique<TThread>(this, index);
        }

        StartLatch_.Wait();
    }

    void Shutdown()
    {
        for (const auto& thread : Threads_) {
            thread->Shutdown();
        }

        LibraryLock_.Reset();
    }

    TGrpcLibraryLockPtr CreateLibraryLock()
    {
        return New<TGrpcLibraryLock>();
    }

    grpc_completion_queue* PickRandomCompletionQueue()
    {
        return Threads_[RandomNumber<size_t>() % ThreadCount]->GetCompletionQueue();
    }

private:
    class TThread
    {
    public:
        TThread(TImpl* owner, int index)
            : Owner_(owner)
            , Index_(index)
            , CompletionQueue_(grpc_completion_queue_create_for_next(nullptr))
            , Thread_(&TThread::ThreadMainTrampoline, this)
        {
            Thread_.Start();
        }

        grpc_completion_queue* GetCompletionQueue()
        {
            return CompletionQueue_.Unwrap();
        }

        void Shutdown()
        {
            grpc_completion_queue_shutdown(CompletionQueue_.Unwrap());
            Thread_.Join();
            CompletionQueue_.Reset();
        }

    private:
        TImpl* const Owner_;
        const int Index_;

        TGrpcCompletionQueuePtr CompletionQueue_;

        ::TThread Thread_;

        static void* ThreadMainTrampoline(void* opaque)
        {
            static_cast<TThread*>(opaque)->ThreadMain();
            return nullptr;
        }

        void ThreadMain()
        {
            auto threadName = Format("Grpc:%v", Index_);
            ::TThread::CurrentThreadSetName(threadName.c_str());

            LOG_DEBUG("Dispatcher thread started (Name: %v)", threadName);

            Owner_->StartLatch_.CountDown();

            bool done = false;
            while (!done) {
                auto event = grpc_completion_queue_next(
                    CompletionQueue_.Unwrap(),
                    gpr_inf_future(GPR_CLOCK_REALTIME),
                    nullptr);
                switch (event.type) {
                    case GRPC_OP_COMPLETE:
                        if (event.tag) {
                            auto* typedTag = reinterpret_cast<TCompletionQueueTag*>(reinterpret_cast<intptr_t>(event.tag) & ~7);
                            auto cookie = reinterpret_cast<intptr_t>(event.tag) & 7;
                            typedTag->Run(event.success != 0, cookie);
                        }
                        break;

                    case GRPC_QUEUE_SHUTDOWN:
                        done = true;
                        break;

                    default:
                        Y_UNREACHABLE();
                }
            }

            LOG_DEBUG("Dispatcher thread stopped (Name: %v)", threadName);
        }
    };

    std::vector<std::unique_ptr<TThread>> Threads_;
    TCountDownLatch StartLatch_;

    TGrpcLibraryLockPtr LibraryLock_;

};

////////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : Impl_(new TImpl())
{ }

TDispatcher::~TDispatcher() = default;

TDispatcher* TDispatcher::Get()
{
    return Singleton<TDispatcher>();
}

void TDispatcher::StaticShutdown()
{
    Get()->Impl_->Shutdown();
}

TGrpcLibraryLockPtr TDispatcher::CreateLibraryLock()
{
    return Impl_->CreateLibraryLock();
}

grpc_completion_queue* TDispatcher::PickRandomCompletionQueue()
{
    return Impl_->PickRandomCompletionQueue();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(7, TDispatcher::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
