#pragma once

#include "common.h"
#include "../rpc/client.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCellManager
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TCellManager> TPtr;

    struct TConfig
    {
        TConfig()
            : Id(InvalidMasterId)
        {}

        yvector<Stroka> MasterAddresses;
        TMasterId Id;
    };

    TCellManager(const TConfig& config);

    TMasterId GetSelfId() const;
    i32 GetMasterCount() const;
    i32 GetQuorum() const;

    // TODO: move to inl
    template <class TProxy>
    TAutoPtr<TProxy> GetMasterProxy(TMasterId id) const
    {
        return new TProxy(ChannelCache.GetChannel(Config.MasterAddresses.at(id)));
    }

private:
    TConfig Config;
    mutable NRpc::TChannelCache ChannelCache;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
