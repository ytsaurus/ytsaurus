#include "stdafx.h"
#include "channel_cache.h"

#include <ytlib/misc/thread_affinity.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

TChannelCache::TChannelCache()
    : IsTerminated(false)
{ }

IChannel::TPtr TChannelCache::GetChannel(const Stroka& address)
{
    VERIFY_THREAD_AFFINITY_ANY();

    // NB: double-checked locking.
    TGuard<TSpinLock> firstAttemptGuard(SpinLock);

    YASSERT(!IsTerminated);

    auto it = ChannelMap.find(address);
    if (it == ChannelMap.end()) {
        firstAttemptGuard.Release();
        auto channel = CreateBusChannel(address);

        TGuard<TSpinLock> secondAttemptGuard(SpinLock);
        it = ChannelMap.find(address);
        if (it == ChannelMap.end()) {
            it = ChannelMap.insert(MakePair(address, channel)).First();
        } else {
            channel->Terminate();
        }
    }

    return it->Second();
}

void TChannelCache::Shutdown()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(SpinLock);

    if (IsTerminated)
        return;

    IsTerminated  = true;

    FOREACH (const auto& pair, ChannelMap) {
        pair.Second()->Terminate();
    }

    ChannelMap.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
