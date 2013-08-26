#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

#include <ytlib/rpc/public.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellManager
    : public TRefCounted
{
public:
    explicit TCellManager(TCellConfigPtr config);

    void Initialize();

    int GetQuorum() const;
    int GetPeerCount() const;

    const Stroka& GetPeerAddress(TPeerId id) const;
    NRpc::IChannelPtr GetMasterChannel(TPeerId id) const;

    DEFINE_BYVAL_RO_PROPERTY(TPeerId, SelfId);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, SelfAddress);

private:
    TCellConfigPtr Config;
    std::vector<Stroka> OrderedAddresses;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT

