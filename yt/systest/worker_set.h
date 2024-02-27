#pragma once

#include <library/cpp/yt/logging/logger.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <random>

namespace NYT::NTest {

class TWorkerSet {
public:
    typedef std::function<std::vector<TString>()> TFetcher;

    TWorkerSet(IInvokerPtr invoker, TFetcher fecher, TDuration pollDelay, TDuration failureBackoff);
    ~TWorkerSet();

    void Start();

    // Must release all previously acquired workers before calling Stop()
    void Stop();

private:
    friend class TWorkerSetTest;

    struct TWorker
    {
        TWorker(TString hostPort)
            : HostPort(hostPort)
            , Missing(false)
            , InUse(false)
            , NumFailures(0)
            , NumPermanentFailures(0)
            , LastFailure(TInstant::Seconds(0))
        {
        }

        TString HostPort;
        bool Missing;
        bool InUse;
        int NumFailures;
        int NumPermanentFailures;
        TInstant LastFailure;
    };

    struct TToken
    {
        TToken();
        TToken(const TToken&) = default;
        TToken& operator=(const TToken&) = default;

        const TString& HostPort() const { return Worker_->HostPort; }
        void MarkFailure();
        void MarkPermanentFailure();
        void Release();

   private:
        friend class TWorkerSet;

        TWorkerSet* Owner_;
        TWorkerSet::TWorker* Worker_;
        bool Failure_;
        bool PermanentFailure_;

        TToken(TWorkerSet* owner, TWorkerSet::TWorker* worker);
    };

    NLogging::TLogger Logger;
    IInvokerPtr Invoker_;
    TDuration PollDelay_;
    TDuration FailureBackoff_;
    TFetcher Fetcher_;

    std::atomic<bool> Stopping_;
    TPromise<void> PollerDone_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::mt19937_64 Engine_;
    std::vector<std::unique_ptr<TWorker>> Workers_;
    std::unordered_map<TString, int> WorkerIndex_;
    std::deque<TPromise<TToken>> Waiters_;

    TFuture<TToken> PickWorker();
    bool IsPermanentFailure(const TWorker& worker);
    bool CanUseWorker(const TWorker& worker, TInstant currentTime);
    void UpdateWorkers(const std::vector<TString>& current);
    void PollingLoop();

public:
    struct TWorkerGuard
    {
        ~TWorkerGuard();

        TWorkerGuard() = default;

        TWorkerGuard(const TWorkerGuard&) = delete;
        TWorkerGuard& operator=(const TWorkerGuard&) = delete;

        TWorkerGuard(TWorkerGuard&&) noexcept;
        TWorkerGuard& operator=(TWorkerGuard&&) noexcept;

        const TString& HostPort() const;
        void MarkFailure(bool permanent);

        void Reset();

    private:
        std::optional<TWorkerSet::TToken> Token_;

        TWorkerGuard(TWorkerSet::TToken token);
        friend class TWorkerSet;
    };

    // Potentially blocks waiting for an available worker.
    TWorkerGuard AcquireWorker();
};

}  // namespace NYT::NTest
