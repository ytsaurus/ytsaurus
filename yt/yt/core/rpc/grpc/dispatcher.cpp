#include "dispatcher.h"

#include "config.h"

#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/concurrency/count_down_latch.h>
#include <yt/yt/core/concurrency/thread.h>

#include <yt/yt/core/misc/shutdown_priorities.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>

#include <contrib/libs/grpc/src/core/lib/iomgr/executor.h>

#include <atomic>

namespace NYT::NRpc::NGrpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = GrpcLogger;

////////////////////////////////////////////////////////////////////////////////

void* TCompletionQueueTag::GetTag(int cookie)
{
    YT_ASSERT(cookie >= 0 && cookie < 8);
    return reinterpret_cast<void*>(reinterpret_cast<intptr_t>(this) | cookie);
}

////////////////////////////////////////////////////////////////////////////////

static std::atomic<int> GrpcLibraryRefCounter;
static std::atomic<int> GrpcLibraryInitCounter;

TGrpcLibraryLock::TGrpcLibraryLock()
{
    if (GrpcLibraryRefCounter.fetch_add(1) == 0) {
        // Failure here indicates an attempt to re-initialize GRPC after shutdown.
        YT_VERIFY(GrpcLibraryInitCounter.fetch_add(1) == 0);
        YT_LOG_DEBUG("Initializing GRPC library");
        grpc_init_openssl();
        grpc_init();
    }
}

TGrpcLibraryLock::~TGrpcLibraryLock()
{
    if (GrpcLibraryRefCounter.fetch_sub(1) == 1) {
        YT_LOG_DEBUG("Shutting down GRPC library");
        grpc_shutdown();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:

    void Configure(const TDispatcherConfigPtr& config)
    {
        auto guard = Guard(ConfigureLock_);

        if (Configured_.load()) {
            THROW_ERROR_EXCEPTION("GPRC dispatcher is already configured");
        }

        DoConfigure(config);
    }

    TGrpcLibraryLockPtr CreateLibraryLock()
    {
        EnsureConfigured();
        return New<TGrpcLibraryLock>();
    }

    TGuardedGrpcCompletitionQueuePtr* PickRandomGuardedCompletionQueue()
    {
        EnsureConfigured();
        return Threads_[RandomNumber<size_t>() % Threads_.size()]->GetGuardedCompletionQueue();
    }

private:
    class TDispatcherThread
        : public TThread
    {
    public:
        explicit TDispatcherThread(int index)
            : TThread(
                Format("Grpc:%v", index),
                EThreadPriority::Normal,
                GrpcDispatcherThreadShutdownPriority)
            , GuardedCompletionQueue_(TGrpcCompletionQueuePtr(grpc_completion_queue_create_for_next(nullptr)))
        {
            Start();
        }

        TGuardedGrpcCompletitionQueuePtr* GetGuardedCompletionQueue()
        {
            return &GuardedCompletionQueue_;
        }

    private:
        TGrpcLibraryLockPtr LibraryLock_ = New<TGrpcLibraryLock>();
        TGuardedGrpcCompletitionQueuePtr GuardedCompletionQueue_;

        void StopPrologue() override
        {
            GuardedCompletionQueue_.Shutdown();
        }

        void StopEpilogue() override
        {
            GuardedCompletionQueue_.Reset();
            LibraryLock_.Reset();
        }

        void ThreadMain() override
        {
            YT_LOG_INFO("Dispatcher thread started");

            // Take raw completion queue for fetching tasks,
            // because `grpc_completion_queue_next` can be concurrent with other operations.
            grpc_completion_queue* completionQueue = GuardedCompletionQueue_.UnwrapUnsafe();

            bool done = false;
            while (!done) {
                auto event = grpc_completion_queue_next(
                    completionQueue,
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
                        YT_ABORT();
                }
            }

            YT_LOG_INFO("Dispatcher thread stopped");
        }
    };

    using TDispatcherThreadPtr = TIntrusivePtr<TDispatcherThread>;

    void EnsureConfigured()
    {
        if (Configured_.load()) {
            return;
        }

        auto guard = Guard(ConfigureLock_);

        if (Configured_.load()) {
            return;
        }

        DoConfigure(New<TDispatcherConfig>());
    }

    void DoConfigure(const TDispatcherConfigPtr& config)
    {
        VERIFY_SPINLOCK_AFFINITY(ConfigureLock_);
        YT_VERIFY(!Configured_.load());

        grpc_core::Executor::SetThreadsLimit(config->GrpcThreadCount);

        for (int index = 0; index < config->DispatcherThreadCount; ++index) {
            Threads_.push_back(New<TDispatcherThread>(index));
        }
        Configured_.store(true);
    }


    std::atomic<bool> Configured_ = false;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ConfigureLock_);
    std::vector<TDispatcherThreadPtr> Threads_;
};

////////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : Impl_(std::make_unique<TImpl>())
{ }

TDispatcher::~TDispatcher() = default;

TDispatcher* TDispatcher::Get()
{
    return LeakySingleton<TDispatcher>();
}

void TDispatcher::Configure(const TDispatcherConfigPtr& config)
{
    Impl_->Configure(config);
}

TGrpcLibraryLockPtr TDispatcher::CreateLibraryLock()
{
    return Impl_->CreateLibraryLock();
}

TGuardedGrpcCompletitionQueuePtr* TDispatcher::PickRandomGuardedCompletionQueue()
{
    return Impl_->PickRandomGuardedCompletionQueue();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
