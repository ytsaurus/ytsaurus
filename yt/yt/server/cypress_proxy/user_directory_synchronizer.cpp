#include "user_directory_synchronizer.h"

#include "dynamic_config_manager.h"
#include "private.h"
#include "user_directory.h"

#include <yt/yt/client/api/etc_client.h>

#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/per_user_request_queue_provider.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TUserDirectorySynchronizer
    : public IUserDirectorySynchronizer
{
public:
    TUserDirectorySynchronizer(
        TUserDirectorySynchronizerConfigPtr config,
        NApi::IClientPtr client,
        TUserDirectoryPtr userDirectory,
        IInvokerPtr invoker,
        TCallbackList<void(const TString&)>* reconfigurationCallback)
    : Config_(std::move(config))
    , Client_(std::move(client))
    , UserDirectory_(std::move(userDirectory))
    , ReconfigurationCallback_(reconfigurationCallback)
    , SyncExecutor_(New<TPeriodicExecutor>(
        invoker,
        BIND(&TUserDirectorySynchronizer::OnSync, MakeStrong(this)),
        Config_->SyncPeriod))
    { }

    void Start() override
    {
        auto guard = Guard(SpinLock_);
        DoStart();
    }

    void Stop() override
    {
        auto guard = Guard(SpinLock_);
        DoStop();
    }

    TFuture<void> NextSync(bool synchronizeImmediately) override
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return MakeFuture(TError("User directory synchronizer is stopped"));
        }
        DoStart(synchronizeImmediately);
        return NextSyncPromise_.ToFuture();
    }

    TFuture<void> RecentSync() override
    {
        auto guard = Guard(SpinLock_);
        if (Stopped_) {
            return MakeFuture(TError("User directory synchronizer is stopped"));
        }
        DoStart(false);
        return RecentSyncPromise_.ToFuture();
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TError&), Synchronized);

private:
    const TUserDirectorySynchronizerConfigPtr Config_;
    NApi::IClientPtr Client_;
    const TWeakPtr<TUserDirectory> UserDirectory_;
    TCallbackList<void(const TString&)>* ReconfigurationCallback_;

    const TPeriodicExecutorPtr SyncExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    bool Started_ = false;
    bool Stopped_ = false;

    TPromise<void> NextSyncPromise_ = NewPromise<void>();
    TPromise<void> RecentSyncPromise_ = NewPromise<void>();


    void DoStart(bool synchronizeImmediately = false)
    {
        if (Started_) {
            if (synchronizeImmediately) {
                SyncExecutor_->ScheduleOutOfBand();
            }
            return;
        }
        Started_ = true;
        SyncExecutor_->Start();
        SyncExecutor_->ScheduleOutOfBand();
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
        try {
            TGetClusterMetaOptions options;
            options.ReadFrom = EMasterChannelKind::Cache;
            options.PopulateUserDirectory = true;

            auto meta = WaitFor(Client_->GetClusterMeta(options))
                .ValueOrThrow();

            std::vector<TUserDescriptor> userLimits;
            userLimits.reserve(meta.UserDirectory->limits().size());
            for (const auto& userInfo : meta.UserDirectory->limits()) {
                userLimits.push_back(NYT::FromProto<TUserDescriptor>(userInfo));
            }

            auto userDirectory = UserDirectory_.Lock();
            if (!userDirectory) {
                THROW_ERROR_EXCEPTION("User directory is not available");
            }

            auto updatedUsers = userDirectory->LoadFrom(userLimits);

            for (const auto& userName : updatedUsers) {
                ReconfigurationCallback_->Fire(userName);
            }

            YT_LOG_DEBUG("Finished synchronizing user directory");
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error synchronizing user directory")
                << ex;
        }
    }

    void RenewSyncPromises()
    {
        auto recentSyncPromise = NewPromise<void>();
        auto nextSyncPromise = NewPromise<void>();

        auto guard = Guard(SpinLock_);
        std::swap(nextSyncPromise, NextSyncPromise_);
        std::swap(recentSyncPromise, RecentSyncPromise_);
    }

    void OnSync()
    {
        TError error;
        try {
            DoSync();
            Synchronized_.Fire(TError());
        } catch (const std::exception& ex) {
            error = TError(ex);
            Synchronized_.Fire(error);
            YT_LOG_DEBUG(error);
        }

        auto nextSyncPromise = NextSyncPromise_;

        if (!RecentSyncPromise_.IsSet()) {
            RecentSyncPromise_.Set(error);
        }
        RenewSyncPromises();
        nextSyncPromise.Set(error);
        RecentSyncPromise_.Set(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

IUserDirectorySynchronizerPtr CreateUserDirectorySynchronizer(
    TUserDirectorySynchronizerConfigPtr config,
    NApi::IClientPtr client,
    TUserDirectoryPtr userDirectory,
    IInvokerPtr invoker,
    TCallbackList<void(const TString&)>* reconfigurationCallback)
{
    return New<TUserDirectorySynchronizer>(
        std::move(config),
        std::move(client),
        std::move(userDirectory),
        std::move(invoker),
        std::move(reconfigurationCallback));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
