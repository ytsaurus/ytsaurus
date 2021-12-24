#include "exec_node_admin_service.h"

#include "bootstrap.h"
#include "private.h"
#include "slot_manager.h"
#include "slot_location.h"

#include <yt/yt/ytlib/exec_node_admin/exec_node_admin_service_proxy.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NExecNode {

using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TExecNodeAdminService
    : public TServiceBase
{
public:
    TExecNodeAdminService(IBootstrap* bootstrap)
        : TServiceBase(
            bootstrap->GetControlInvoker(),
            TExecNodeAdminServiceProxy::GetDescriptor(),
            ExecNodeLogger)
        , Bootstrap_(bootstrap)
    {
        YT_VERIFY(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(HealNode));
    }

private:
    IBootstrap* const Bootstrap_;

    DECLARE_RPC_SERVICE_METHOD(NProto, HealNode)
    {
        context->SetRequestInfo("Locations: %v, AlertTypesToReset: %v, ForceReset: %v",
            request->locations(),
            request->alert_types_to_reset(),
            request->force_reset());

        auto slotManager = Bootstrap_->GetSlotManager();

        auto locations = slotManager->GetLocations();

        THashSet<TString> requestLocations(
            request->locations().begin(),
            request->locations().end());

        std::vector<TFuture<void>> futures;
        futures.reserve(locations.size());

        for (auto& location : locations) {
            auto it = requestLocations.find(location->GetId());
            if (it != requestLocations.end()) {
                futures.push_back(location->Repair());
                requestLocations.erase(it);
            }
        }

        if (!requestLocations.empty()) {
            THROW_ERROR_EXCEPTION("Unknown location: %Qv", requestLocations);
        }

        THashSet<TString> alertTypesToReset(
            request->alert_types_to_reset().begin(),
            request->alert_types_to_reset().end());
        for (const auto& alertTypeString : alertTypesToReset) {
            auto alertType = ParseEnum<ESlotManagerAlertType>(alertTypeString);
            if (!IsSlotManagerAlertEligibleToReset(alertType) && !request->force_reset()) {
                THROW_ERROR_EXCEPTION("Alert %Qv is not eligible to reset",
                    FormatEnum(alertType));
            }
            slotManager->ResetAlert(alertType);
        }

        context->ReplyFrom(AllSucceeded(futures));
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateExecNodeAdminService(IBootstrap* bootstrap)
{
    return New<TExecNodeAdminService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
