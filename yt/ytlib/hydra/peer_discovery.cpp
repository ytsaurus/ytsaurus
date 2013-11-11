#include "stdafx.h"
#include "peer_discovery.h"
#include "hydra_service_proxy.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/thread_affinity.h>

#include <core/actions/invoker_util.h>

#include <core/rpc/channel_cache.h>
#include <core/rpc/helpers.h>

#include <util/random/random.h>

namespace NYT {
namespace NHydra {

using namespace NYTree;
using namespace NConcurrency;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HydraLogger;
static TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

class TPeerDiscovery
    : public TRefCounted
{
public:
    explicit TPeerDiscovery(
        TPeerDiscoveryConfigPtr config,
        EPeerRole role)
        : Config(config)
        , Role(role)
        , PromiseLock(false)
        , Promise(NewPromise<TErrorOr<TPeerDiscoveryResult>>())
        , Awaiter(New<TParallelAwaiter>(GetSyncInvoker()))
    { }

    TFuture<TErrorOr<TPeerDiscoveryResult>> Run()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto promise = Promise;
        auto awaiter = Awaiter;

        for (const Stroka& address : Config->Addresses) {
            LOG_DEBUG("Requesting quorum information from peer %s", ~address);

            auto busChannel = ChannelCache.GetChannel(address);
            auto realmChannel = CreateRealmChannel(busChannel, Config->CellGuid);
            THydraServiceProxy proxy(realmChannel);
            proxy.SetDefaultTimeout(Config->RpcTimeout);

            auto request = proxy.GetQuorum();

            awaiter->Await(
                request->Invoke(),
                BIND(&TPeerDiscovery::OnResponse, MakeStrong(this), address));
        }

        awaiter->Complete(
            BIND(&TPeerDiscovery::OnComplete, MakeStrong(this)));

        return promise;
    }

private:
    TPeerDiscoveryConfigPtr Config;
    EPeerRole Role;

    TAtomic PromiseLock;
    TPromise<TErrorOr<TPeerDiscoveryResult>> Promise;
    TParallelAwaiterPtr Awaiter;

    bool AcquireLock()
    {
        return AtomicCas(&PromiseLock, true, false);
    }

    void SetPromise(const TErrorOr<TPeerDiscoveryResult>& result)
    {
        Promise.Set(result);
        Awaiter->Cancel();
    }

    void OnResponse(const Stroka& address, THydraServiceProxy::TRspGetQuorumPtr response)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!response->IsOK()) {
            LOG_WARNING(response->GetError(), "Error requesting quorum information from peer %s",
                ~address);
            return;
        }

        LOG_DEBUG("Received quorum information from peer %s (EpochId: %s, LeaderAddress: %s, FollowerAddresses: [%s])",
            ~address,
            ~ToString(FromProto<TEpochId>(response->epoch_id())),
            ~response->leader_address(),
            ~JoinToString(response->follower_addresses()));

        if (!AcquireLock())
            return;

        TPeerDiscoveryResult result;
        result.EpochId = FromProto<TEpochId>(response->epoch_id());
        switch (Role) {
            case EPeerRole::Any: {
                int id = RandomNumber<unsigned int>(1 + response->follower_addresses_size());
                if (id == response->follower_addresses().size()) {
                    result.Address = response->leader_address();
                } else {
                    result.Address = response->follower_addresses().Get(id);
                }
                break;
            }

            case EPeerRole::Leader:
                result.Address = response->leader_address();
                break;

            case EPeerRole::Follower: {
                if (response->follower_addresses().size() == 0) {
                    result.Address = response->leader_address();
                } else {
                    int id = RandomNumber<unsigned int>(response->follower_addresses_size());
                    result.Address = response->follower_addresses().Get(id);
                }
                break;
            }

            default:
                YUNREACHABLE();
        }

        SetPromise(result);
    }

    void OnComplete()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!AcquireLock())
            return;

        LOG_WARNING("No quorum information received");

        SetPromise(TErrorOr<TPeerDiscoveryResult>(TError(
            NHydra::EErrorCode::NoQuorum,
            "No quorum")));
    }

};

TFuture<TErrorOr<TPeerDiscoveryResult>> DiscoverPeer(
    TPeerDiscoveryConfigPtr config,
    EPeerRole role)
{
    auto discovery = New<TPeerDiscovery>(config, role);
    return discovery->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
