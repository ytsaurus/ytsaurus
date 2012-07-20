#include "stdafx.h"
#include "master_discovery.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/rpc/channel_cache.h>

#include <util/random/random.h>

namespace NYT {
namespace NElection {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ElectionLogger;
static NProfiling::TProfiler Profiler("/election/master_discovery");
static NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

TMasterDiscovery::TMasterDiscovery(TConfigPtr config)
    : Config(config)
{ }

TMasterDiscovery::TAsyncResult TMasterDiscovery::GetMaster()
{
    return GetQuorum().Apply(
        BIND([] (TProxy::TRspGetQuorumPtr quorum) -> TAsyncResult {
            TResult result;
            if (quorum) {
                int id = RandomNumber<unsigned int>(1 + quorum->follower_addresses_size());
                if (id == quorum->follower_addresses().size()) {
                    result.Address = quorum->leader_address();
                } else {
                    result.Address = quorum->follower_addresses().Get(id);
                }
                result.Epoch = TGuid::FromProto(quorum->epoch());
            }
            return MakeFuture(MoveRV(result)); 
        })
    );
}

TMasterDiscovery::TAsyncResult TMasterDiscovery::GetLeader()
{
    return GetQuorum().Apply(
        BIND([] (TProxy::TRspGetQuorumPtr quorum) -> TAsyncResult {
            TResult result;
            if (quorum) {
                result.Address = quorum->leader_address();
                result.Epoch = TGuid::FromProto(quorum->epoch());
            }
            return MakeFuture(MoveRV(result));
        })
    );
}

TMasterDiscovery::TAsyncResult TMasterDiscovery::GetFollower()
{
    return GetQuorum().Apply(
        BIND([] (TProxy::TRspGetQuorumPtr quorum) -> TAsyncResult {
            TResult result;
            if (quorum) {
                int id = RandomNumber<unsigned int>(quorum->follower_addresses_size());
                result.Address = quorum->follower_addresses().Get(id);
                result.Epoch = TGuid::FromProto(quorum->epoch());
            }
            return MakeFuture(MoveRV(result)); 
        })
    );
}

TFuture<TMasterDiscovery::TProxy::TRspGetQuorumPtr> TMasterDiscovery::GetQuorum()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto promise = NewPromise<TProxy::TRspGetQuorumPtr>();
    auto awaiter = New<TParallelAwaiter>(&Profiler, "/time");

    FOREACH (Stroka address, Config->Addresses) {
        LOG_DEBUG("Requesting quorum information from peer %s", ~address);

        TProxy proxy(ChannelCache.GetChannel(address));
        proxy.SetDefaultTimeout(Config->RpcTimeout);

        auto request = proxy.GetQuorum();
        awaiter->Await(
            request->Invoke(),
            EscapeYPathToken(address),
            BIND(
                &TMasterDiscovery::OnResponse,
                MakeStrong(this),
                awaiter,
                promise,
                address));
    }

    awaiter->Complete(BIND(
        &TMasterDiscovery::OnComplete,
        MakeStrong(this),
        promise));

    return promise;
}

void TMasterDiscovery::OnResponse(
    TParallelAwaiterPtr awaiter,
    TPromise<TProxy::TRspGetQuorumPtr> promise,
    const Stroka& address,
    TProxy::TRspGetQuorumPtr response)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!response->IsOK()) {
        LOG_WARNING("Error requesting quorum information from peer %s\n%s",
            ~address,
            ~response->GetError().ToString());
        return;
    }

    LOG_DEBUG("Received quorum information from peer %s (Epoch: %s, LeaderAddress: %s, FollowerAddresses: [%s])",
        ~address,
        ~TEpoch::FromProto(response->epoch()).ToString(),
        ~response->leader_address(),
        ~JoinToString(response->follower_addresses()));

    TGuard<TSpinLock> guard(SpinLock);    
    if (promise.IsSet())
        return;

    promise.Set(MoveRV(response));

    awaiter->Cancel();
}

void TMasterDiscovery::OnComplete(TPromise<TProxy::TRspGetQuorumPtr> promise)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);    
    if (promise.IsSet())
        return;

    promise.Set(NULL);

    LOG_INFO("No quorum information received");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
