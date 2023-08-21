#include "mutation_forwarder_thunk.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

void TMutationForwarderThunk::MaybeForwardMutationToSiblingServant(
    TTabletId tabletId,
    const ::google::protobuf::Message& message)
{
    Underlying_->MaybeForwardMutationToSiblingServant(tabletId, message);
}

void TMutationForwarderThunk::SetUnderlying(IMutationForwarderPtr underlying)
{
    Underlying_ = std::move(underlying);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
