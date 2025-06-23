#include "master_connector.h"

#include "private.h"

#include "bootstrap.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/server/lib/sequoia/cypress_proxy_tracker_service_proxy.h>

#include <yt/yt/server/lib/sequoia/proto/cypress_proxy_tracker.pb.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/sequoia_client/sequoia_reign.h>

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

constinit const auto Logger = CypressProxyLogger;

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

    bool IsUp() const override
    {
        return RegistrationError_.Read([] (const TError& error) {
            return
                error.IsOK() ||
                error.GetCode() == NSequoiaClient::EErrorCode::InvalidSequoiaReign;
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

    int GetMaxCopiableSubtreeSize() const override
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

    void Heartbeat()
    {
        auto connection = Bootstrap_->GetNativeConnection();
        auto channel = connection->GetMasterChannelOrThrow(
            EMasterChannelKind::Follower,
            PrimaryMasterCellTagSentinel);
        auto proxy = TCypressProxyTrackerServiceProxy(std::move(channel));
        auto request = proxy.Heartbeat();
        request->set_address(SelfAddress_);
        request->set_sequoia_reign(ToProto(GetCurrentSequoiaReign()));
        request->set_version(GetVersion());

        auto rspOrError = WaitFor(request->Invoke());
        if (!rspOrError.IsOK()) {
            if (rspOrError.FindMatching(NSequoiaClient::EErrorCode::InvalidSequoiaReign)) {
                YT_LOG_ALERT(rspOrError, "Failed to send heartbeat (CurrentSequoiaReign: %v, Version: %v)",
                    GetCurrentSequoiaReign(),
                    GetVersion());
            } else {
                YT_LOG_ERROR(rspOrError, "Failed to send heartbeat (CurrentSequoiaReign: %v, Version: %v)",
                    GetCurrentSequoiaReign(),
                    GetVersion());
            }

            auto error = WrapCypressProxyRegistrationError(std::move(rspOrError));

            RegistrationError_.Store(std::move(error));
            MasterReign_.Store(0);
            MaxCopiableSubtreeSize_.Store(0);

            return;
        }

        auto rsp = rspOrError.Value();
        auto reign = rsp->master_reign();
        MasterReign_.Store(reign);
        MaxCopiableSubtreeSize_.Store(rsp->limits().max_copiable_subtree_size());

        if (!RegistrationError_.Exchange(TError{}).IsOK()) {
            YT_LOG_DEBUG("Cypress proxy registered at primary master (MasterReign: %v)", reign);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap)
{
    return New<TMasterConnector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
