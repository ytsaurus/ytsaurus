#include "cell_directory_synchronizer.h"
#include "private.h"

#include "cell_directory.h"
#include "config.h"

#include <yt/yt/ytlib/object_client/master_ypath_proxy.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NCellMasterClient {

using namespace NApi;
using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CellMasterClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer
    : public ICellDirectorySynchronizer
{
public:
    TCellDirectorySynchronizer(
        TCellDirectorySynchronizerConfigPtr config,
        ICellDirectoryPtr directory)
        : Config_(std::move(config))
        , Directory_(std::move(directory))
        , SyncExecutor_(New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetLightInvoker(),
            BIND(&TCellDirectorySynchronizer::OnSync, MakeWeak(this)),
            Config_->SyncPeriod))
    { }

    void Start() override
    {
        auto guard = Guard(SpinLock_);
        DoStart(false);
    }

    void Stop() override
    {
        auto guard = Guard(SpinLock_);
        DoStop();
    }

    TFuture<void> NextSync(bool force) override
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return MakeFuture(TError("Master cell directory synchronizer is stopped"));
        }
        DoStart(force);
        return NextSyncPromise_.ToFuture();
    }

    TFuture<void> RecentSync() override
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
    const ICellDirectoryPtr Directory_;

    const TPeriodicExecutorPtr SyncExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
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
        YT_UNUSED_FUTURE(SyncExecutor_->Stop());
    }

    void DoSync()
    {
        // TODO(cherepashka) remove after testing.
        TTraceContextGuard traceContextGuard(GetOrCreateTraceContext("MasterCellDirectory"));
        try {
            YT_LOG_DEBUG("Started synchronizing master cell directory");

            // NB: Here, we count on the directory being able to provide us with a
            // channel to primary cell even before the first sync happens.

            auto primaryMasterChannel = Directory_->GetMasterChannelOrThrow(EMasterChannelKind::Cache);
            auto proxy = TObjectServiceProxy::FromDirectMasterChannel(std::move(primaryMasterChannel));

            auto batchReq = proxy.ExecuteBatch();
            batchReq->SetSuppressTransactionCoordinatorSync(true);

            auto req = TMasterYPathProxy::GetClusterMeta();
            req->set_populate_cell_directory(true);

            auto* cachingHeaderExt = req->Header().MutableExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
            cachingHeaderExt->set_expire_after_successful_update_time(ToProto(Config_->ExpireAfterSuccessfulUpdateTime));
            cachingHeaderExt->set_expire_after_failed_update_time(ToProto(Config_->ExpireAfterFailedUpdateTime));

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

            YT_LOG_DEBUG("Finished synchronizing master cell directory");
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error synchronizing cell directory")
                << ex;
        }
    }

    void OnSync()
    {
        TError error;
        auto period = Config_->SyncPeriod;
        try {
            NTracing::TNullTraceContextGuard nullTraceContext;
            DoSync();
        } catch (const std::exception& ex) {
            error = ex;
            YT_LOG_WARNING(error);
            if (Config_->RetryPeriod) {
                period = Config_->RetryPeriod;
            }
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

ICellDirectorySynchronizerPtr CreateCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    ICellDirectoryPtr directory)
{
    return New<TCellDirectorySynchronizer>(
        std::move(config),
        std::move(directory));
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT::NCellMasterClient
