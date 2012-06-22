#include "stdafx.h"
#include "channel_cache.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/foreach.h>
#include <ytlib/rpc/bus_channel.h>
#include <ytlib/bus/config.h>
#include <ytlib/bus/tcp_client.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

TChannelCache::TChannelCache()
    : IsTerminated(false)
{ }

TChannelCache::~TChannelCache()
{ }

IChannelPtr TChannelCache::GetChannel(const Stroka& address)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // NB: double-checked locking.
    TGuard<TSpinLock> firstAttemptGuard(SpinLock);

    YASSERT(!IsTerminated);

    auto it = ChannelMap.find(address);
    if (it == ChannelMap.end()) {
        firstAttemptGuard.Release();

        auto config = New<TTcpBusClientConfig>(address);
        auto client = CreateTcpBusClient(config);
        auto channel = CreateBusChannel(client);

        TGuard<TSpinLock> secondAttemptGuard(SpinLock);
        it = ChannelMap.find(address);
        if (it == ChannelMap.end()) {
            it = ChannelMap.insert(MakePair(address, channel)).first;
        } else {
            channel->Terminate();
        }
    }

    return it->second;
}

void TChannelCache::Shutdown()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);

    if (IsTerminated)
        return;

    IsTerminated  = true;

    FOREACH (const auto& pair, ChannelMap) {
        pair.second->Terminate();
    }

    ChannelMap.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
