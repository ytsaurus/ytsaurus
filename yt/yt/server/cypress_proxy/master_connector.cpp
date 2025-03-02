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

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
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

    bool IsRegistered() override
    {
        return RegistrationError_.Read([] (const TError& error) {
            return error.IsOK();
        });
    }

    void ValidateRegistration() override
    {
        RegistrationError_.Read([] (const TError& error) {
            error.ThrowOnError();
        });
    }

private:
    IBootstrap* const Bootstrap_;
    const std::string SelfAddress_;
    const TPeriodicExecutorPtr Executor_;

    TAtomicObject<TError> RegistrationError_;

    void Heartbeat()
    {
        auto connection = Bootstrap_->GetNativeConnection();
        auto proxy = TCypressProxyTrackerServiceProxy(
            connection->GetMasterChannelOrThrow(
                EMasterChannelKind::Leader,
                PrimaryMasterCellTagSentinel));
        auto request = proxy.Heartbeat();
        request->set_address(SelfAddress_);
        request->set_sequoia_reign(ToProto(GetCurrentSequoiaReign()));

        auto error = WaitFor(request->Invoke());

        if (!error.IsOK()) {
            if (error.FindMatching(NSequoiaClient::EErrorCode::InvalidSequoiaReign)) {
                YT_LOG_ALERT(error, "Failed to send heartbeat; retrying");
            } else {
                YT_LOG_ERROR(error, "Failed to send heartbeat; retrying");
            }

            error = WrapCypressProxyRegistrationError(std::move(error));
        } else if (!IsRegistered()) {
            YT_LOG_DEBUG("Cypress proxy registered at primary master");
        }

        RegistrationError_.Store(std::move(error));
    }
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap)
{
    return New<TMasterConnector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
