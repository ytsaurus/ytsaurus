#include "stdafx.h"
#include "master_discovery.h"
#include "config.h"
#include "private.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/rpc/channel_cache.h>

#include <util/random/random.h>

namespace NYT {
namespace NMetaState {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;
static NProfiling::TProfiler Profiler("/meta_state/master_discovery");
static NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

class TMasterDiscovery::TQuorumRequester
    : public TRefCounted
{
public:
    explicit TQuorumRequester(TMasterDiscoveryConfigPtr config)
        : Config(config)
        , PromiseLatch(0)
        , Promise(NewPromise<TProxy::TRspGetQuorumPtr>())
        , Awaiter(New<TParallelAwaiter>(&Profiler, "/time"))
    { }

    TFuture<TMasterDiscovery::TProxy::TRspGetQuorumPtr> Run()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        FOREACH (const Stroka& address, Config->Addresses) {
            LOG_DEBUG("Requesting quorum information from peer %s", ~address);

            TProxy proxy(ChannelCache.GetChannel(address));
            proxy.SetDefaultTimeout(Config->RpcTimeout);

            auto request = proxy.GetQuorum();
            Awaiter->Await(
                request->Invoke(),
                EscapeYPathToken(address),
                BIND(&TQuorumRequester::OnResponse, MakeStrong(this), address));
        }

        Awaiter->Complete(BIND(&TQuorumRequester::OnComplete, MakeStrong(this)));

        return Promise;
    }

private:
    TMasterDiscoveryConfigPtr Config;

    TAtomic PromiseLatch;
    TPromise<TMasterDiscovery::TProxy::TRspGetQuorumPtr> Promise;
    TParallelAwaiterPtr Awaiter;

    bool AcquireLatch()
    {
        return AtomicIncrement(PromiseLatch) == 1;
    }

    void OnResponse(const Stroka& address, TProxy::TRspGetQuorumPtr response)
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

        if (!AcquireLatch())
            return;

        Promise.Set(MoveRV(response));
        Awaiter->Cancel();
        Awaiter.Reset();
    }

    void OnComplete()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!AcquireLatch())
            return;

        LOG_INFO("No quorum information received");

        Promise.Set(NULL);
        Awaiter.Reset();
    }
};

TMasterDiscovery::TMasterDiscovery(TMasterDiscoveryConfigPtr config)
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
    return New<TQuorumRequester>(Config)->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
