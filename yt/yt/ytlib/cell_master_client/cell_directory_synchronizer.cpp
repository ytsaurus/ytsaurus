#include "cell_directory_synchronizer.h"
#include "private.h"

#include "cell_directory.h"
#include "config.h"

#include <yt/ytlib/object_client/master_ypath_proxy.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/rpc/dispatcher.h>
#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NCellMasterClient {

using namespace NApi;
using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NObjectClient;

///////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TCellDirectorySynchronizerConfigPtr config,
        TCellDirectoryPtr directory)
        : Config_(std::move(config))
        , Directory_(std::move(directory))
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TImpl::OnSync, MakeWeak(this)),
            Config_->SyncPeriod))
    { }

    void Start()
    {
        auto guard = Guard(SpinLock_);
        DoStart(false);
    }

    void Stop()
    {
        auto guard = Guard(SpinLock_);
        DoStop();
    }

    TFuture<void> NextSync(bool force)
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return MakeFuture(TError("Master cell directory synchronizer is stopped"));
        }
        DoStart(force);
        return NextSyncPromise_.ToFuture();
    }

    TFuture<void> RecentSync()
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return MakeFuture(TError("Master cell directory synchronizer is stopped"));
        }
        DoStart(false);
        return RecentSyncPromise_.ToFuture();
    }

    DEFINE_SIGNAL(void(const TError&), Synchronized);

private:
    const TCellDirectorySynchronizerConfigPtr Config_;
    TCellDirectoryPtr Directory_;

    const TPeriodicExecutorPtr SyncExecutor_;

    TSpinLock SpinLock_;
    bool Started_ = false;
    bool Stopped_= false;
    TPromise<void> NextSyncPromise_ = NewPromise<void>();
    TPromise<void> RecentSyncPromise_ = NewPromise<void>();

    void DoStart(bool force)
    {
        if (Started_) {
            if (force) {
                SyncExecutor_->ScheduleOutOfBand();
            }
            return;
        }
        Started_ = true;
        SyncExecutor_->Start();
    }

    void DoStop()
    {
        if (Stopped_) {
            return;
        }
        Stopped_ = true;
        SyncExecutor_->Stop();
    }

    void DoSync()
    {
        // NB: here, we count on the directory being able to provide us with a
        // channel to primary cell even before the first sync happens.

        auto primaryMasterChannel = Directory_->GetMasterChannelOrThrow(EMasterChannelKind::Cache);
        TObjectServiceProxy proxy(primaryMasterChannel);

        auto batchReq = proxy.ExecuteBatch();

        auto req = TMasterYPathProxy::GetClusterMeta();
        req->set_populate_cell_directory(true);

        auto* cachingHeaderExt = req->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
        cachingHeaderExt->set_success_expiration_time(ToProto<i64>(Config_->SuccessExpirationTime));
        cachingHeaderExt->set_failure_expiration_time(ToProto<i64>(Config_->FailureExpirationTime));

        batchReq->AddRequest(req);

        auto batchRsp = WaitFor(batchReq->Invoke())
            .ValueOrThrow();

        // TODO(shakurov): Should we have a weak pointer to Directory_?

        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspGetClusterMeta>(0)
            .Value();

        // COMPAT(shakurov): support old masters' empty responses.
        if (rsp->has_cell_directory()) {
            Directory_->Update(rsp->cell_directory());
        } else {
            Directory_->UpdateDefault();
        }
    }

    void OnSync()
    {
        TError error;
        std::optional<TDuration> period;
        try {
            DoSync();

            period = Config_->SyncPeriod;
            YT_LOG_DEBUG("Synchronizing master cell directory succeeded, next sync in %v", period);
        } catch (const std::exception& ex) {
            error = TError("Synchronizing master cell directory failed") << ex;

            period = Config_->RetryPeriod ? Config_->RetryPeriod : Config_->SyncPeriod;
            YT_LOG_WARNING(error, "Synchronizing master cell directory failed, next sync in %v", period);
        }

        SyncExecutor_->SetPeriod(period);

        auto nextSyncPromise = NextSyncPromise_;
        // Don't drop the very first recent sync promise.
        if (!RecentSyncPromise_.IsSet()) {
            RecentSyncPromise_.Set(error);
        }
        RenewSyncPromises();

        nextSyncPromise.Set(error);
        RecentSyncPromise_.Set(error);
    }

    void RenewSyncPromises()
    {
        auto recentSyncPromise = NewPromise<void>();
        auto nextSyncPromise = NewPromise<void>();

        auto guard = Guard(SpinLock_);
        std::swap(nextSyncPromise, NextSyncPromise_);
        std::swap(recentSyncPromise, RecentSyncPromise_);
    }

};

////////////////////////////////////////////////////////////////////////////////

TCellDirectorySynchronizer::TCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    TCellDirectoryPtr directory)
    : Impl_(New<TCellDirectorySynchronizer::TImpl>(
        std::move(config),
        std::move(directory)))
{ }

TCellDirectorySynchronizer::~TCellDirectorySynchronizer()
{ }

void TCellDirectorySynchronizer::Start()
{
    Impl_->Start();
}

void TCellDirectorySynchronizer::Stop()
{
    Impl_->Stop();
}

TFuture<void> TCellDirectorySynchronizer::NextSync(bool force)
{
    return Impl_->NextSync(force);
}

TFuture<void> TCellDirectorySynchronizer::RecentSync()
{
    return Impl_->RecentSync();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
