#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

struct TCellPeerConfig
    : public NYTree::TYsonStruct
{
    std::optional<std::string> Address;
    std::optional<std::string> AlienCluster;
    bool Voting;

    REGISTER_YSON_STRUCT(TCellPeerConfig);

    static void Register(TRegistrar registrar);
};
DEFINE_REFCOUNTED_TYPE(TCellPeerConfig)

void FormatValue(TStringBuilderBase* builder, const TCellPeerConfigPtr& config, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TCellConfig
    : public NYTree::TYsonStruct
{
    //! Cell id; an arbitrary random object id of |Cell| type.
    TCellId CellId;

    //! Peer addresses.
    //! Some could be Null to indicate that the peer is temporarily missing.
    std::vector<TCellPeerConfigPtr> Peers;

    void ValidateAllPeersPresent();

    int CountVotingPeers() const;

    int FindPeerId(const std::string& address) const;
    int GetPeerIdOrThrow(const std::string& address) const;

    REGISTER_YSON_STRUCT(TCellConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
