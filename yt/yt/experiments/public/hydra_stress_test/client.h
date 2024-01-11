#pragma once

#include "public.h"
#include "config.h"
#include "checkers.h"
#include "peer.h"
#include "peer_proxy.h"

#include <yt/yt/core/misc/public.h>

namespace NYT::NHydraStressTest {

//////////////////////////////////////////////////////////////////////////////////

class TClient
    : public TRefCounted
{
public:
    TClient(
        TConfigPtr config,
        NRpc::IChannelPtr peerChannel,
        IInvokerPtr invoker,
        TLivenessCheckerPtr livenessChecker,
        int clientId);

    void Run();

private:
    const TConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const TLivenessCheckerPtr LivenessChecker_;
    const NLogging::TLogger Logger;
    const TConsistencyCheckerPtr ConsistencyChecker_;

    TPeerServiceProxy Proxy_;

    void RunRead();
    void RunCas();
    void RunSequence();

    void DoRun();
};

DEFINE_REFCOUNTED_TYPE(TClient)

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
