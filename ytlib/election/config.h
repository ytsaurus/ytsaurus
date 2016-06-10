#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

struct TCellPeerConfig
{
    TCellPeerConfig();
    explicit TCellPeerConfig(const TNullable<Stroka>& address, bool voting = true);

    TNullable<Stroka> Address;
    bool Voting = true;
};

Stroka ToString(const TCellPeerConfig& config);

void Serialize(const TCellPeerConfig& config, NYson::IYsonConsumer* consumer);
void Deserialize(TCellPeerConfig& config, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

class TCellConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Cell id; an arbitrary random object id of |Cell| type.
    TCellId CellId;

    //! Peer addresses.
    //! Some could be |Null| to indicate that the peer is temporarily missing.
    std::vector<TCellPeerConfig> Peers;

    TCellConfig();

    void ValidateAllPeersPresent();

};

DEFINE_REFCOUNTED_TYPE(TCellConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
