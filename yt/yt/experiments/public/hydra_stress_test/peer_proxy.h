#pragma once

#include "public.h"
#include "automaton.h"
#include "config.h"

#include <yt/yt/core/rpc/client.h>

#include <yt/yt/experiments/public/hydra_stress_test/peer_service.pb.h>

namespace NYT::NHydraStressTest {

//////////////////////////////////////////////////////////////////////////////////

class TPeerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TPeerServiceProxy, PeerService);

    DEFINE_RPC_PROXY_METHOD(NYT::NProto, Read);
    DEFINE_RPC_PROXY_METHOD(NYT::NProto, Cas);
    DEFINE_RPC_PROXY_METHOD(NYT::NProto, Sequence);
};

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
