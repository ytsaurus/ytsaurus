#include "timestamp_manager.h"
#include "private.h"
#include "config.h"

#include <yt/server/election/election_manager.h>

#include <yt/server/hydra/composite_automaton.h>
#include <yt/server/hydra/hydra_manager.h>
#include <yt/server/hydra/mutation.h>

#include <yt/server/transaction_server/timestamp_manager.pb.h>

#include <yt/client/transaction_client/timestamp_service_proxy.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/serialize.h>

#include <yt/core/rpc/server.h>
#include <yt/core/rpc/service_detail.h>

#include <time.h>

namespace NYT::NTransactionServer {

using namespace NRpc;
using namespace NHydra;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NTransactionServer::NProto;

////////////////////////////////////////////////////////////////////////////////

class TTimestampManager::TImpl
    : public TServiceBase
    , public TCompositeAutomatonPart
{
public:
    TImpl(
        TTimestampManagerConfigPtr config,
        IInvokerPtr automatonInvoker,
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton)
        : TServiceBase(
            // Ignored, method handlers use TimestampInvoker_.
            GetSyncInvoker(),
            TTimestampServiceProxy::GetDescriptor(),
            TransactionServerLogger)
        , TCompositeAutomatonPart(
            hydraManager,
            automaton,
            automatonInvoker)
        , Config_(config)
    {
        YCHECK(Config_);
        YCHECK(AutomatonInvoker_);

        TimestampQueue_ = New<TActionQueue>("Timestamp");
        TimestampInvoker_ = TimestampQueue_->GetInvoker();

        VERIFY_INVOKER_THREAD_AFFINITY(TimestampInvoker_, TimestampThread);
        VERIFY_INVOKER_THREAD_AFFINITY(AutomatonInvoker_, AutomatonThread);

        CalibrationExecutor_ = New<TPeriodicExecutor>(
            TimestampInvoker_,
            BIND(&TImpl::Calibrate, Unretained(this)),
            Config_->CalibrationPeriod);
        CalibrationExecutor_->Start();

        TServiceBase::RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateTimestamps)
            .SetInvoker(TimestampInvoker_));

        RegisterLoader(
            "TimestampManager",
            BIND(&TImpl::Load, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "TimestampManager",
            BIND(&TImpl::Save, Unretained(this)));

        TCompositeAutomatonPart::RegisterMethod(
            BIND(&TImpl::HydraCommitTimestamp, Unretained(this)));
    }

    IServicePtr GetRpcService()
    {
        return this;
    }

private:
    const TTimestampManagerConfigPtr Config_;

    TActionQueuePtr TimestampQueue_;
    IInvokerPtr TimestampInvoker_;
    
    TPeriodicExecutorPtr CalibrationExecutor_;

    // Timestamp thread affinity:
    
    //! Can we generate timestamps?
    volatile bool Active_ = false;

    //! Are we backing off because no committed timestamps are available?
    //! Used to avoid repeating same logging message.
    bool BackingOff_ = false;

    //! First unused timestamp.
    TTimestamp CurrentTimestamp_ = NullTimestamp;

    //! Last committed timestamp as viewed by the timestamp thread.
    //! All generated timestamps must be less than this one.
    TTimestamp CommittedTimestamp_ = NullTimestamp;


    // Automaton thread affinity:

    //! Last committed timestamp as viewed by the automaton.
    TTimestamp PersistentTimestamp_;


    DECLARE_THREAD_AFFINITY_SLOT(TimestampThread);
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    // RPC handlers.

    DECLARE_RPC_SERVICE_METHOD(NTransactionClient::NProto, GenerateTimestamps)
    {
        VERIFY_THREAD_AFFINITY(TimestampThread);

        context->SetRequestInfo("Count: %v", request->count());

        DoGenerateTimestamps(context);
    }

    void DoGenerateTimestamps(const TCtxGenerateTimestampsPtr& context)
    {
        VERIFY_THREAD_AFFINITY(TimestampThread);

        if (!Active_) {
            context->Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Timestamp provider is not active"));
            return;
        }

        int count = context->Request().count();
        YCHECK(count >= 0);
        if (count > Config_->MaxTimestampsPerRequest) {
            context->Reply(TError("Too many timestamps requested: %v > %v",
                count,
                Config_->MaxTimestampsPerRequest));
            return;
        }

        if (CurrentTimestamp_ + count >= CommittedTimestamp_) {
            // Backoff and retry.
            LOG_WARNING_UNLESS(BackingOff_, "Not enough spare timestamps; backing off");
            BackingOff_ = true;
            TDelayedExecutor::Submit(
                BIND(&TImpl::DoGenerateTimestamps, MakeStrong(this), context)
                    .Via(TimestampInvoker_),
                Config_->RequestBackoffTime);
            return;
        }

        if (BackingOff_) {
            LOG_INFO("Spare timestamps are available again");
            BackingOff_ = false;
        }
        
        // Make sure there's no overflow in the counter part.
        YCHECK(((CurrentTimestamp_ + count) >> TimestampCounterWidth) == (CurrentTimestamp_ >> TimestampCounterWidth));

        auto result = CurrentTimestamp_;
        CurrentTimestamp_ += count;

        context->SetResponseInfo("Timestamp: %llx", result);

        context->Response().set_timestamp(result);
        context->Reply();
    }


    void Calibrate()
    {
        VERIFY_THREAD_AFFINITY(TimestampThread);

        if (!Active_)
            return;

        ui64 nowSeconds = ::time(nullptr);
        ui64 prevSeconds = (CurrentTimestamp_ >> TimestampCounterWidth);
        
        if (nowSeconds == prevSeconds)
            return;

        if (nowSeconds < prevSeconds) {
            LOG_WARNING("Clock went back, keeping current timestamp (PrevSeconds: %v, NowSeconds: %v)",
                prevSeconds,
                nowSeconds);
            return;
        }

        CurrentTimestamp_ = (nowSeconds << TimestampCounterWidth);
        LOG_DEBUG("Timestamp advanced (Timestamp: %llx)",
            CurrentTimestamp_);

        auto commitTimestamp =
            CurrentTimestamp_ +
            (Config_->CommitAdvance.Seconds() << TimestampCounterWidth);

        TReqCommitTimestamp request;
        request.set_timestamp(commitTimestamp);

        auto mutation = CreateMutation(HydraManager_, request);
        BIND([mutation = std::move(mutation)] {
            return mutation->Commit();
        })
            .AsyncVia(AutomatonInvoker_)
            .Run()
            .Subscribe(BIND(&TImpl::OnTimestampCommitted, MakeStrong(this), commitTimestamp)
                .Via(TimestampInvoker_));
    }

    void OnTimestampCommitted(TTimestamp timestamp, const TErrorOr<TMutationResponse>& result)
    {
        VERIFY_THREAD_AFFINITY(TimestampThread);

        if (!result.IsOK()) {
            LOG_ERROR(result, "Error committing timestamp");
            return;
        }

        CommittedTimestamp_ = timestamp;

        LOG_DEBUG("Timestamp committed (Timestamp: %llx)",
            CommittedTimestamp_);
    }


    virtual void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::Clear();

        PersistentTimestamp_ = NullTimestamp;
    }

    void Load(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        PersistentTimestamp_ = NYT::Load<TTimestamp>(context);
    }

    void Save(TSaveContext& context) const
    {
        NYT::Save(context, PersistentTimestamp_);
    }


    virtual void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::OnLeaderActive();

        LOG_INFO("Persistent timestamp is %v",
            PersistentTimestamp_);

        auto persistentTimestamp = PersistentTimestamp_;
        auto invoker = HydraManager_
            ->GetAutomatonCancelableContext()
            ->CreateInvoker(TimestampInvoker_);

        auto callback = BIND([=, this_ = MakeStrong(this)] () {
            VERIFY_THREAD_AFFINITY(TimestampThread);

            Active_ = true;
            CurrentTimestamp_ = persistentTimestamp;
            CommittedTimestamp_ = persistentTimestamp;

            LOG_INFO("Timestamp generator is now active (Timestamp: %llx)",
                persistentTimestamp);
        }).Via(invoker);

        ui64 deadlineSeconds = PersistentTimestamp_ >> TimestampCounterWidth;
        ui64 nowSeconds = ::time(nullptr);
        if (nowSeconds > deadlineSeconds) {
            callback.Run();
        } else {
            auto delay = TDuration::Seconds(deadlineSeconds - nowSeconds + 1); // +1 to be sure
            LOG_INFO("Timestamp generation postponed for %v ms to ensure monotonicity",
                delay);
            TDelayedExecutor::Submit(callback, delay);
        }
    }

    virtual void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TCompositeAutomatonPart::OnStopLeading();

        TimestampInvoker_->Invoke(BIND([=, this_ = MakeStrong(this)] () {
            VERIFY_THREAD_AFFINITY(TimestampThread);

            if (!Active_)
                return;

            Active_ = false;
            CurrentTimestamp_ = NullTimestamp;
            CommittedTimestamp_ = NullTimestamp;

            LOG_INFO("Timestamp generator is no longer active");
        }));
    }


    void HydraCommitTimestamp(TReqCommitTimestamp* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        PersistentTimestamp_ = request->timestamp();

        LOG_DEBUG_UNLESS(IsRecovery(), "Persistent timestamp updated (Timestamp: %llx)",
            PersistentTimestamp_);
    }


    virtual bool IsUp(const TCtxDiscoverPtr& /*context*/) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Active_;
    }
};

////////////////////////////////////////////////////////////////////////////////

TTimestampManager::TTimestampManager(
    TTimestampManagerConfigPtr config,
    IInvokerPtr automatonInvoker,
    IHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton)
    : Impl_(New<TImpl>(
        config,
        automatonInvoker,
        hydraManager,
        automaton))
{ }

TTimestampManager::~TTimestampManager() = default;

IServicePtr TTimestampManager::GetRpcService()
{
    return Impl_->GetRpcService();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
