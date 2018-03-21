#pragma once

#include "public.h"

#include <yt/core/net/public.h>

namespace NYP {
namespace NServer {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

void ValidateNodeShortName(const TString& name);
TString BuildDefaultShortNodeName(const TString& id);
void ValidatePodFqdn(const TString& fqdn);
void ValidateMtnNetwork(const NYT::NNet::TIP6Network& network);
THostSubnet HostSubnetFromMtnAddress(const NYT::NNet::TIP6Address& address);
TProjectId ProjectIdFromMtnAddress(const NYT::NNet::TIP6Address& address);
TNonce NonceFromMtnAddress(const NYT::NNet::TIP6Address& address);
NYT::NNet::TIP6Address MakeMtnAddress(
    THostSubnet hostSubnet,
    TProjectId projectId,
    TNonce nonce);
NYT::NNet::TIP6Network MakeMtnNetwork(
    THostSubnet hostSubnet,
    TNonce nonce);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NServer
} // namespace NYP
