#include "helpers.h"
#include "hydra_service_proxy.h"
#include "private.h"

namespace NYT::NHydra {

using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

void SwitchLeader(
    const std::vector<IChannelPtr>& peerChannels,
    const IChannelPtr& currentLeaderChannel,
    const IChannelPtr& newLeaderChannel,
    const std::vector<TString>& addresses,
    const TString& newLeaderAddress,
    const std::optional<TDuration>& timeout,
    const std::optional<TString>& user)
{
    {
        YT_LOG_INFO("Validating new leader");

        THydraServiceProxy proxy(newLeaderChannel);
        auto req = proxy.GetPeerState();
        req->SetTimeout(timeout);

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            auto state = FromProto<EPeerState>(rspOrError.Value()->peer_state());
            if (state != EPeerState::Following) {
                THROW_ERROR_EXCEPTION("Invalid peer state: expected %Qlv, found %Qlv",
                    EPeerState::Following,
                    state);
            }
        } else {
            auto error = TError(rspOrError);
            // COMPAT(gritukan)
            if (error.FindMatching(NRpc::EErrorCode::NoSuchMethod)) {
                YT_LOG_INFO("Remote Hydra is too old, leader validation cannot be performed");
            } else {
                THROW_ERROR error;
            }
        }
    }

    {
        YT_LOG_INFO("Preparing switch at current leader");

        THydraServiceProxy proxy(currentLeaderChannel);
        auto req = proxy.PrepareLeaderSwitch();
        req->SetTimeout(timeout);

        WaitFor(req->Invoke())
            .ValueOrThrow();
    }

    {
        YT_LOG_INFO("Synchronizing new leader with the current one");

        THydraServiceProxy proxy(newLeaderChannel);
        auto req = proxy.ForceSyncWithLeader();
        req->SetTimeout(timeout);

        WaitFor(req->Invoke())
            .ValueOrThrow();
    }

    TError restartReason(
        "Switching leader to %v by %Qv request",
        newLeaderAddress,
        user);

    {
        YT_LOG_INFO("Restarting new leader with priority boost armed");

        THydraServiceProxy proxy(newLeaderChannel);
        auto req = proxy.ForceRestart();
        req->SetTimeout(timeout);
        ToProto(req->mutable_reason(), restartReason);
        req->set_arm_priority_boost(true);

        WaitFor(req->Invoke())
            .ValueOrThrow();
    }

    {
        YT_LOG_INFO("Restarting all other peers");

        YT_VERIFY(peerChannels.size() == addresses.size());
        for (int peerId = 0; peerId < std::ssize(peerChannels); ++peerId) {
            const auto& peerAddress = addresses[peerId];
            if (peerAddress == newLeaderAddress) {
                continue;
            }

            THydraServiceProxy proxy(peerChannels[peerId]);
            auto req = proxy.ForceRestart();
            req->SetTimeout(timeout);
            ToProto(req->mutable_reason(), restartReason);

            // Fire-and-forget.
            YT_UNUSED_FUTURE(req->Invoke());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
