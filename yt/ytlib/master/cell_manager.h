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

    template <class TProxy>
    TAutoPtr<TProxy> GetMasterProxy(TMasterId id) const;

private:
    TConfig Config;
    mutable NRpc::TChannelCache ChannelCache;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CELL_MANAGER_INL_H_
#include "cell_manager-inl.h"
#undef CELL_MANAGER_INL_H_
