#pragma once

#include "common.h"

#include <ytlib/misc/configurable.h>


namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct TCellConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TCellConfig> TPtr;

    //! Master server addresses.
    yvector<Stroka> Addresses;

    //! The current master server id.
    TPeerId Id;

    TCellConfig()
    {
        Register("id", Id).Default(NElection::InvalidPeerId);
        Register("addresses", Addresses).NonEmpty();
    }

    virtual void DoValidate() const
    {
        if (Id == NElection::InvalidPeerId) {
            ythrow yexception() << "Missing peer id";
        }
        if (Id < 0 || Id >= Addresses.ysize()) {
            ythrow yexception() << Sprintf("Id must be in range 0..%d", Addresses.ysize() - 1);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
