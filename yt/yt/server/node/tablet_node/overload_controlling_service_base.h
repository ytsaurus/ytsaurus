#pragma once

#include "public.h"

#include <yt/yt/core/rpc/service_detail.h>


namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TBaseService>
class TOverloadControllingServiceBase
    : public TBaseService
{
public:
    template <typename... TArgs>
    TOverloadControllingServiceBase(NTabletNode::IBootstrap* bootstrap, TArgs&&... args);

protected:
    void HandleRequest(
        std::unique_ptr<NRpc::NProto::TRequestHeader> header,
        TSharedRefArray message,
        NBus::IBusPtr replyBus) override;

private:
    NTabletNode::IBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#define OVERLOAD_CONTROLLING_SERVICE_BASE_INL_H_
#include "overload_controlling_service_base-inl.h"
#undef OVERLOAD_CONTROLLING_SERVICE_BASE_INL_H_
