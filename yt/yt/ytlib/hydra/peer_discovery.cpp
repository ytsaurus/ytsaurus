#include "peer_discovery.h"

#include <yt/yt/ytlib/hydra/proto/hydra_service.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NHydra {

using namespace NThreading;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class THydraDiscoverRequestHook
    : public IDiscoverRequestHook
{
public:
    explicit THydraDiscoverRequestHook(EPeerKind kind)
        : Kind_(kind)
    { }

    void EnrichRequest(NRpc::NProto::TReqDiscover* request) const
    {
        auto* ext = request->MutableExtension(NProto::TPeerKindExt::peer_kind_ext);
        ext->set_peer_kind(static_cast<int>(Kind_));
    }

    void OnResponse(NRpc::NProto::TRspDiscover* response) const
    {
        if (Kind_ != EPeerKind::Leader) {
            return;
        }

        if (response->HasExtension(NProto::TDiscombobulationExt::discombobulation_ext)) {
            const auto& ext = response->GetExtension(NProto::TDiscombobulationExt::discombobulation_ext);
            if (ext.discombobulated()) {
                auto error = TError(EErrorCode::ReadOnly,
                    "Read-only mode is active");
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::GlobalDiscoveryError,
                    "Cell is in a discombobulated state")
                    << error;
            }
        }
    }

private:
    const EPeerKind Kind_;
};

////////////////////////////////////////////////////////////////////////////////

IDiscoverRequestHookPtr CreateHydraDiscoverRequestHook(EPeerKind kind)
{
    return New<THydraDiscoverRequestHook>(kind);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
