#include "exec_node_admin_service.h"

#include "bootstrap.h"
#include "private.h"
#include "slot_manager.h"
#include "slot_location.h"

#include <yt/yt/ytlib/exec_node_admin/exec_node_admin_service_proxy.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NExecNode {

using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TExecNodeAdminService
    : public TServiceBase
{
public:
    explicit TExecNodeAdminService(IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TExecNodeAdminServiceProxy::GetDescriptor(),
            ExecNodeLogger,
            NullRealmId,
            bootstrap->GetNativeAuthenticator())
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(HealNode));
    }

private:
    IBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NProto, HealNode)
    {
        auto slotManager = Bootstrap_->GetSlotManager();

        context->SetRequestInfo("Locations: %v, AlertTypesToReset: %v, ForceReset: %v, HasFatalAlert: %v",
            request->locations(),
            request->alert_types_to_reset(),
            request->force_reset(),
            slotManager->HasFatalAlert());

        THashSet<TString> alertTypesToReset(
            request->alert_types_to_reset().begin(),
            request->alert_types_to_reset().end());
        for (const auto& alertTypeString : alertTypesToReset) {
            auto alertType = ParseEnum<ESlotManagerAlertType>(alertTypeString);
            if (!TSlotManager::IsResettableAlertType(alertType) && !request->force_reset()) {
                THROW_ERROR_EXCEPTION("Alert %Qlv is not resettable",
                    alertType);
            }
        }

        auto locations = slotManager->GetLocations();

        THashSet<TString> locationIds;
        for (const auto& location : locations) {
            locationIds.insert(location->GetId());
        }

        THashSet<TString> requestLocationIds(
            request->locations().begin(),
            request->locations().end());
        for (const auto& locationId : requestLocationIds) {
            if (!locationIds.contains(locationId)) {
                THROW_ERROR_EXCEPTION("Healing requested for unknown location")
                    << TErrorAttribute("location", locationId);
            }
        }

        std::vector<TFuture<void>> repairFutures;
        repairFutures.reserve(locations.size());

        if (slotManager->HasFatalAlert()) {
            // In case of fatal alert every slot can be in inconsistent state and must be forcefully repaired.
            for (auto& location : locations) {
                repairFutures.push_back(location->Repair(/*force*/ true));
            }
        } else {
            for (auto& location : locations) {
                if (!requestLocationIds.contains(location->GetId())) {
                    continue;
                }
                repairFutures.push_back(location->Repair(/*force*/ false));
            }
        }

        WaitFor(AllSucceeded(repairFutures))
            .ThrowOnError();

        std::vector<ESlotManagerAlertType> alertTypes;
        alertTypes.reserve(alertTypesToReset.size());

        for (const auto& alertTypeString : alertTypesToReset) {
            alertTypes.push_back(ParseEnum<ESlotManagerAlertType>(alertTypeString));
        }

        slotManager->ResetAlerts(alertTypes);

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateExecNodeAdminService(IBootstrap* bootstrap)
{
    return New<TExecNodeAdminService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
