#pragma once

#include "common.h"
#include "channel.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Caches TChannel instances by address.
/*!
 *  \note Thread affinity: any.
 */
class TChannelCache
    : private TNonCopyable
{
public:
    //! Creates a new instance.
    TChannelCache();

    //! Constructs new or gets an earlier created channel for a given address.
    IChannelPtr GetChannel(const Stroka& address);

    //! Shuts down all channels.
    /*!
     *  It is safe to call this method multiple times.
     *  After the first call the instance is no longer usable.
     */
    void Shutdown();

private:
    typedef yhash_map<Stroka, IChannelPtr> TChannelMap;

    bool IsTerminated;
    TChannelMap ChannelMap;
    //! Protects #IsTerminated and #ChannelMap.
    TSpinLock SpinLock;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
