#pragma once

#include "mutation_forwarder.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TMutationForwarderThunk
    : public IMutationForwarder
{
public:
    void MaybeForwardMutationToSiblingServant(
        TTabletId tabletId,
        const ::google::protobuf::Message& message) override;

    void SetUnderlying(IMutationForwarderPtr underlying);

private:
    IMutationForwarderPtr Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TMutationForwarderThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
