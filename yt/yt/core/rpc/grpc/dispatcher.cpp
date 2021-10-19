#include "dispatcher.h"
#include "helpers.h"

#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/concurrency/count_down_latch.h>
#include <yt/yt/core/concurrency/thread.h>

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
    TImpl()
    {
        for (int index = 0; index < ThreadCount; ++index) {
            Threads_.push_back(New<TDispatcherThread>(index));
        }
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
    class TDispatcherThread
        : public TThread
    {
    public:
        explicit TDispatcherThread(int index)
            : TThread(Format("Grpc:%v", index))
        {
            Start();
        }

        grpc_completion_queue* GetCompletionQueue()
        {
            return CompletionQueue_.Unwrap();
        }

    private:
        TGrpcLibraryLockPtr LibraryLock_ = New<TGrpcLibraryLock>();
        TGrpcCompletionQueuePtr CompletionQueue_ = TGrpcCompletionQueuePtr(grpc_completion_queue_create_for_next(nullptr));


        void StopPrologue() override
        {
            grpc_completion_queue_shutdown(CompletionQueue_.Unwrap());
        }

        void StopEpilogue() override
        {
            CompletionQueue_.Reset();
            LibraryLock_.Reset();
        }

        void ThreadMain() override
        {
            YT_LOG_INFO("Dispatcher thread started");

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
                        YT_ABORT();
                }
            }

            YT_LOG_INFO("Dispatcher thread stopped");
        }
    };

    using TDispatcherThreadPtr = TIntrusivePtr<TDispatcherThread>;

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

TGrpcLibraryLockPtr TDispatcher::CreateLibraryLock()
{
    return Impl_->CreateLibraryLock();
}

grpc_completion_queue* TDispatcher::PickRandomCompletionQueue()
{
    return Impl_->PickRandomCompletionQueue();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
