#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellPeerConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<TString> Address;
    std::optional<TString> AlienCluster;
    bool Voting;

    REGISTER_YSON_STRUCT(TCellPeerConfig);

    static void Register(TRegistrar registrar);
};
DEFINE_REFCOUNTED_TYPE(TCellPeerConfig)

TString ToString(const TCellPeerConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

class TCellConfig
    : public NYTree::TYsonStruct
{
public:
    //! Cell id; an arbitrary random object id of |Cell| type.
    TCellId CellId;

    //! Peer addresses.
    //! Some could be Null to indicate that the peer is temporarily missing.
    std::vector<TCellPeerConfigPtr> Peers;

    void ValidateAllPeersPresent();

    int CountVotingPeers() const;

    int FindPeerId(const TString& address) const;
    int GetPeerIdOrThrow(const TString& address) const;

    REGISTER_YSON_STRUCT(TCellConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
