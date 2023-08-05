#include "overload_controlling_service_base.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

std::optional<TDuration> GetTimeout(const std::unique_ptr<NRpc::NProto::TRequestHeader>& header)
{
    return header->has_timeout()
        ? std::make_optional(FromProto<TDuration>(header->timeout()))
        : std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
