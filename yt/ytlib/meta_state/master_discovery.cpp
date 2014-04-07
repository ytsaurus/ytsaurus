#include "stdafx.h"
#include "master_discovery.h"
#include "config.h"
#include "private.h"

#include <core/misc/serialize.h>

#include <core/concurrency/thread_affinity.h>

#include <core/ytree/ypath_client.h>

#include <core/rpc/channel_cache.h>

#include <core/logging/log.h>

#include <util/random/random.h>

namespace NYT {
namespace NMetaState {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = MetaStateLogger;

static NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

class TMasterDiscovery::TQuorumRequester
    : public TRefCounted
{
public:
    struct TQuorumResponse {
        TQuorumResponse()
        { }

        TQuorumResponse(const Stroka& address, const TMasterDiscovery::TProxy::TRspGetQuorumPtr& quorum):
            Address(address),
            Quorum(quorum)
        { }

        Stroka Address;
        TMasterDiscovery::TProxy::TRspGetQuorumPtr Quorum;
    };

    explicit TQuorumRequester(TMasterDiscoveryConfigPtr config)
        : Config(config)
        , PromiseLatch(0)
        , Promise(NewPromise<TQuorumResponse>())
        , Awaiter(New<TParallelAwaiter>(GetSyncInvoker()))
    { }

    TFuture<TQuorumResponse> Run()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto promise = Promise;
        auto awaiter = Awaiter;

        FOREACH (const Stroka& address, Config->Addresses) {
            LOG_DEBUG("Requesting quorum information from peer %s", ~address);

            TProxy proxy(ChannelCache.GetChannel(address));
            proxy.SetDefaultTimeout(Config->RpcTimeout);

            auto request = proxy.GetQuorum();

            awaiter->Await(
                request->Invoke(),
                BIND(&TQuorumRequester::OnResponse, MakeStrong(this), address));
        }

        awaiter->Complete(BIND(&TQuorumRequester::OnComplete, MakeStrong(this)));

        return promise;
    }

private:
    TMasterDiscoveryConfigPtr Config;

    TAtomic PromiseLatch;
    TPromise<TQuorumResponse> Promise;
    TParallelAwaiterPtr Awaiter;

    bool AcquireLatch()
    {
        return AtomicIncrement(PromiseLatch) == 1;
    }

    void OnResponse(const Stroka& address, TProxy::TRspGetQuorumPtr response)
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

        if (!AcquireLatch())
            return;

        Promise.Set(TQuorumResponse(address, response));
        Awaiter->Cancel();
        Cleanup();
    }

    void OnComplete()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!AcquireLatch())
            return;

        LOG_INFO("No quorum information received");

        Promise.Set(TQuorumResponse());
        Cleanup();
    }

    void Cleanup()
    {
        Promise.Reset();
        Awaiter.Reset();
    }
};

TMasterDiscovery::TMasterDiscovery(TMasterDiscoveryConfigPtr config)
    : Config(config)
{ }

TMasterDiscovery::TAsyncResult TMasterDiscovery::GetLeader()
{
    return New<TQuorumRequester>(Config)->Run().Apply(
        BIND([] (TQuorumRequester::TQuorumResponse rsp) -> TAsyncResult {
            TResult result;
            if (rsp.Quorum) {
                result.Address = rsp.Address;
                result.EpochId = FromProto<TEpochId>(rsp.Quorum->epoch_id());
            }
            return MakeFuture(std::move(result));
        })
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
