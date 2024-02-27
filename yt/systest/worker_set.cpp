
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/systest/worker_set.h>

namespace NYT::NTest {

static const int kLargeCapacity = 16384;

static TDuration BackoffDuration(TDuration base, int NumFailures)
{
    return std::min(
        std::max(1, NumFailures - 4) * base,
        30 * base
    );
}

///////////////////////////////////////////////////////////////////////////////

TWorkerSet::TWorkerGuard::TWorkerGuard(TWorkerSet::TToken token)
    : Token_(token)
{
}

TWorkerSet::TWorkerGuard::TWorkerGuard(TWorkerGuard&& other) noexcept
    : Token_(other.Token_)
{
    other.Token_.reset();
}

TWorkerSet::TWorkerGuard&
TWorkerSet::TWorkerGuard::operator=(TWorkerGuard&& other) noexcept
{
    Token_ = other.Token_;
    other.Token_.reset();
    return *this;
}

TWorkerSet::TWorkerGuard::~TWorkerGuard()
{
    Reset();
}

const TString& TWorkerSet::TWorkerGuard::HostPort() const
{
    return Token_->HostPort();
}

void TWorkerSet::TWorkerGuard::MarkFailure(bool permanent)
{
    Token_->MarkFailure();
    if (permanent) {
        Token_->MarkPermanentFailure();
    }
}

void TWorkerSet::TWorkerGuard::Reset()
{
    if (Token_) {
        Token_->Release();
        Token_.reset();
    }
}

///////////////////////////////////////////////////////////////////////////////

TWorkerSet::TToken::TToken(TWorkerSet* owner, TWorkerSet::TWorker* worker)
    : Owner_(owner)
    , Worker_(worker)
    , Failure_(false)
    , PermanentFailure_(false)
{
}

TWorkerSet::TToken::TToken()
    : Owner_(nullptr)
    , Worker_(nullptr)
    , Failure_(false)
    , PermanentFailure_(false)
{
}

void TWorkerSet::TToken::MarkFailure()
{
    Failure_ = true;
}

void TWorkerSet::TToken::MarkPermanentFailure()
{
    PermanentFailure_ = true;
}

void TWorkerSet::TToken::Release()
{
    TPromise<TToken> waiter;
    TInstant failureTime = Failure_ ? TInstant::Now() : TInstant();
    {
        auto guard = Guard(Owner_->Lock_);
        if (Failure_) {
            ++Worker_->NumFailures;
            Worker_->LastFailure = failureTime;
        } else {
            Worker_->NumFailures = 0;
        }
        if (PermanentFailure_) {
            ++Worker_->NumPermanentFailures;
        }
        if (Failure_ || Owner_->Waiters_.empty()) {
            Worker_->InUse = false;
        } else {
            waiter = Owner_->Waiters_.front();
            Owner_->Waiters_.pop_front();
        }
    }

    if (waiter) {
        waiter.Set(TToken(Owner_, Worker_));
    }
    Owner_ = nullptr;
    Worker_ = nullptr;
}

///////////////////////////////////////////////////////////////////////////////

TWorkerSet::TWorkerSet(
    IInvokerPtr invoker,
    TFetcher fetcher,
    TDuration pollDelay,
    TDuration failureBackoff)
    : Logger("worker_set")
    , Invoker_(invoker)
    , PollDelay_(pollDelay)
    , FailureBackoff_(failureBackoff)
    , Fetcher_(fetcher)
    , PollerDone_(NewPromise<void>())
{
    Workers_.reserve(kLargeCapacity);
    Stopping_.store(false);
}

TWorkerSet::~TWorkerSet()
{
    YT_VERIFY(Stopping_.load());
}

void TWorkerSet::Start()
{
    YT_UNUSED_FUTURE(BIND(&TWorkerSet::PollingLoop, this)
        .AsyncVia(Invoker_)
        .Run());
}

void TWorkerSet::Stop()
{
    Stopping_.store(true);
    PollerDone_.Get().ThrowOnError();
}

TWorkerSet::TWorkerGuard TWorkerSet::AcquireWorker()
{
    return TWorkerGuard(NConcurrency::WaitFor(PickWorker()).ValueOrThrow());
}

void TWorkerSet::PollingLoop()
{
    YT_LOG_INFO("Poller started");
    while (!Stopping_.load()) {
        auto workers = Fetcher_();
        UpdateWorkers(workers);
        NConcurrency::TDelayedExecutor::WaitForDuration(PollDelay_);
    }
    YT_LOG_INFO("Poller completed");
    PollerDone_.Set();
}

TFuture<TWorkerSet::TToken> TWorkerSet::PickWorker()
{
    auto timeNow = TInstant::Now();
    auto guard = Guard(Lock_);

    std::mt19937_64 engine(Engine_());
    std::vector<int> available;
    for (int i = 0; i < std::ssize(Workers_); ++i) {
        if (CanUseWorker(*Workers_[i], timeNow)) {
            available.push_back(i);
        }
    }

    if (available.empty()) {
        Waiters_.push_back(NewPromise<TToken>());
        return Waiters_.back();
    }

    int index = available[engine() % std::ssize(available)];
    Workers_[index]->InUse = true;

    return MakeFuture(TToken(this, Workers_[index].get()));
}

void TWorkerSet::UpdateWorkers(const std::vector<TString>& current)
{
    std::vector<int> indexPresent;
    indexPresent.reserve(kLargeCapacity);

    int totalWorkers;
    {
        auto guard = Guard(Lock_);
        for (const auto& hostport : current) {
            auto insertResult = WorkerIndex_.insert(std::pair(hostport, std::ssize(Workers_)));
            if (insertResult.second) {
                indexPresent.push_back(std::ssize(Workers_));
                Workers_.push_back(std::make_unique<TWorker>(hostport));
            } else {
                indexPresent.push_back(insertResult.first->second);
            }
        }

        totalWorkers = std::ssize(Workers_);
    }

    std::sort(indexPresent.begin(), indexPresent.end());

    int numPermanentFailures = 0;
    std::vector<int> available;
    TPromise<TToken> waiter;
    TToken token;
    auto timeNow = TInstant::Now();
    int numWaiters = -1;

    {
        auto guard = Guard(Lock_);
        int pos = 0;
        for (int i = 0; i < totalWorkers; ++i) {
            if (pos == std::ssize(indexPresent) || indexPresent[pos] > i) {
                Workers_[i]->Missing = true;
            } else {
                Workers_[i]->Missing = false;
                if (CanUseWorker(*Workers_[i], timeNow)) {
                    available.push_back(i);
                } else if (IsPermanentFailure(*Workers_[i])) {
                    ++numPermanentFailures;
                }
                ++pos;
            }
        }

        if (!available.empty() && !Waiters_.empty()) {
            std::mt19937_64 engine(Engine_());

            waiter = Waiters_.front();
            Waiters_.pop_front();

            int index = available[engine() % std::ssize(available)];
            Workers_[index]->InUse = true;
            token = TToken(this, Workers_[index].get());
        }
        numWaiters = std::ssize(Waiters_);
    }

    YT_LOG_INFO("Finished worker set update (NumPresent: %v, "
        "NumAvailable: %v, NumPermanentFailures: %v, NumWaiting: %v)",
        std::ssize(indexPresent),
        std::ssize(available),
        numPermanentFailures,
        numWaiters);

    if (waiter) {
        waiter.Set(token);
    }
}

bool TWorkerSet::IsPermanentFailure(const TWorker& worker)
{
    return worker.NumPermanentFailures >= 3;
}

bool TWorkerSet::CanUseWorker(const TWorker& worker, TInstant currentTime)
{
    return !worker.InUse && !worker.Missing &&
        (worker.NumFailures == 0 ||
         currentTime > worker.LastFailure + BackoffDuration(FailureBackoff_, worker.NumFailures)) &&
        !IsPermanentFailure(worker);
}

}  // namespace NYT::NTest
