#include "master_connector.h"

#include "private.h"

#include "bootstrap.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/server/lib/sequoia/cypress_proxy_tracker_service_proxy.h>

#include <yt/yt/server/lib/sequoia/proto/cypress_proxy_tracker.pb.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/build/build.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNet;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NSequoiaServer;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

static const auto Logger = CypressProxyLogger();

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public IMasterConnector
{
public:
    explicit TMasterConnector(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , SelfAddress_(
            BuildServiceAddress(
                GetLocalHostName(),
                Bootstrap_->GetConfig()->RpcPort))
        , Executor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TMasterConnector::Heartbeat, MakeWeak(this)),
            Bootstrap_->GetConfig()->HeartbeatPeriod))
        , RegistrationError_(TError(NRpc::EErrorCode::Unavailable, "Cypress proxy is not registered"))
    { }

    void Start() override
    {
        Executor_->Start();
        Executor_->ScheduleOutOfBand();
    }

    bool IsRegistered() const override
    {
        return RegistrationError_.Read([] (const TError& error) {
            return error.IsOK();
        });
    }

    void ValidateRegistration() const override
    {
        RegistrationError_.Read([] (const TError& error) {
            error.ThrowOnError();
        });
    }

    TReign GetMasterReign() const override
    {
        return MasterReign_.Load();
    }

    int MaxCopiableSubtreeSize() const override
    {
        return MaxCopiableSubtreeSize_.Load();
    }

private:
    IBootstrap* const Bootstrap_;
    const std::string SelfAddress_;
    const TPeriodicExecutorPtr Executor_;

    TAtomicObject<TError> RegistrationError_;
    TAtomicObject<TReign> MasterReign_;
    TAtomicObject<int> MaxCopiableSubtreeSize_;

    // NB: when master is in read-only mode heartbeats should not be sent to
    // leader since there is no active leader.
    bool RequireLeader_ = true;

    void Heartbeat()
    {
        auto connection = Bootstrap_->GetNativeConnection();
        auto channel = connection->GetMasterChannelOrThrow(
            RequireLeader_
                ? EMasterChannelKind::Leader
                : EMasterChannelKind::Follower,
            PrimaryMasterCellTagSentinel);
        auto proxy = TCypressProxyTrackerServiceProxy(std::move(channel));
        auto request = proxy.Heartbeat();
        request->set_address(SelfAddress_);
        request->set_sequoia_reign(ToProto(GetCurrentSequoiaReign()));
        request->set_version(GetVersion());

        // By default require leader to update last seen time.
        RequireLeader_ = true;

        auto rspOrError = WaitFor(request->Invoke());
        if (!rspOrError.IsOK()) {
            if (rspOrError.FindMatching(NSequoiaClient::EErrorCode::InvalidSequoiaReign)) {
                YT_LOG_ALERT(rspOrError, "Failed to send heartbeat; retrying (CurrentSequoiaReign: %v, Version: %v)",
                    GetCurrentSequoiaReign(),
                    GetVersion());
            } else {
                // TODO(kvk1920): Consider to not log "master in read-only mode"
                // many times.
                YT_LOG_ERROR(rspOrError, "Failed to send heartbeat; retrying (CurrentSequoiaReign: %v, Version: %v)",
                    GetCurrentSequoiaReign(),
                    GetVersion());

                // Cypress proxies should remain available during master restart and
                // read-only mode.
                if (auto readOnly = rspOrError.FindMatching(NHydra::EErrorCode::ReadOnly); IsRetriableError(rspOrError) || readOnly) {
                    // Fallback to mutation-less protocol.
                    RequireLeader_ = !readOnly;
                    return;
                }

                auto error = WrapCypressProxyRegistrationError(std::move(rspOrError));

                RegistrationError_.Store(std::move(error));
                MasterReign_.Store(0);
                MaxCopiableSubtreeSize_.Store(0);
            }

            return;
        }

        auto rsp = rspOrError.Value();
        auto reign = rsp->master_reign();
        MasterReign_.Store(reign);
        MaxCopiableSubtreeSize_.Store(rsp->limits().max_copiable_subtree_size());

        if (!IsRegistered()) {
            YT_LOG_DEBUG("Cypress proxy registered at primary master (MasterReign: %v)", reign);
        }

        RegistrationError_.Store(TError{});
    }
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap)
{
    return New<TMasterConnector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
