#pragma once

#include "common.h"

#include "../rpc/client.h"

#include "../misc/config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCellManager
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TCellManager> TPtr;

    struct TConfig
    {
        yvector<Stroka> MasterAddresses;
        TMasterId Id;

        TConfig()
            : Id(InvalidMasterId)
        { }

        void Read(TJsonObject* json)
        {
            NYT::TryRead(json, L"Id", &Id);
            NYT::TryRead(json, L"MasterAddresses", &MasterAddresses);
        }
    };

    TCellManager(const TConfig& config);

    TMasterId GetSelfId() const;
    i32 GetMasterCount() const;
    i32 GetQuorum() const;
    Stroka GetMasterAddress(TMasterId id) const;

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
