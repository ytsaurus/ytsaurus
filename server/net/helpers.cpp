#include "helpers.h"

#include <yp/client/api/public.h>

#include <yt/core/net/address.h>

namespace NYP {
namespace NServer {
namespace NNet {

using namespace NClient::NApi;

using namespace NYT::NNet;

////////////////////////////////////////////////////////////////////////////////

void ValidateNodeShortName(const TString& name)
{
    if (name.empty()) {
        THROW_ERROR_EXCEPTION("Node short name cannot be empty");
    }

    if (name.length() > MaxNodeShortNameLength) {
        THROW_ERROR_EXCEPTION("Node short name %Qv it too long: limit %v, actual %v",
            name,
            MaxNodeShortNameLength,
            name.length());
    }

    static const char ValidChars[] =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "-";
    for (char ch : name) {
        if (!strchr(ValidChars, ch)) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::InvalidObjectId,
                "Node short name %Qv contains invalid symbol %Qv",
                name,
                ch);
        }
    }
}

TString BuildDefaultShortNodeName(const TString& id)
{
    auto index = id.find('.');
    return index == TString::npos ? id : id.substr(0, index);
}

void ValidatePodFqdn(const TString& fqdn)
{
    const size_t Limit = MaxPodFqdnLength;
    if (fqdn.length() > Limit) {
        THROW_ERROR_EXCEPTION("Pod FQDN %Qv it too long: limit %v, actual %v",
            fqdn,
            MaxPodFqdnLength,
            fqdn.length());
    }
}

void ValidateMtnNetwork(const TIP6Network& network)
{
    if (network.GetMaskSize() != 64) {
        THROW_ERROR_EXCEPTION("%v is not a valid MTN network",
            network);
    }
}

THostSubnet HostSubnetFromMtnAddress(const TIP6Address& address)
{
    const auto* parts = address.GetRawDWords();
    return static_cast<THostSubnet>(parts[2]) + (static_cast<THostSubnet>(parts[3]) << 32);
}

TProjectId ProjectIdFromMtnAddress(const TIP6Address& address)
{
    const auto* parts = address.GetRawDWords();
    return parts[1];
}

TNonce NonceFromMtnAddress(const TIP6Address& address)
{
    const auto* parts = address.GetRawDWords();
    return static_cast<TNonce>(parts[0] >> 16);
}

TIP6Address MakeMtnAddress(
    THostSubnet hostSubnet,
    TProjectId projectId,
    TNonce nonce)
{
    ui32 parts[4] = {
        static_cast<ui32>(nonce) << 16,
        projectId,
        static_cast<ui32>(hostSubnet & 0xffffffff),
        static_cast<ui32>(hostSubnet >> 32)
    };
    return TIP6Address::FromRawDWords(parts);
}

TIP6Network MakeMtnNetwork(
    THostSubnet hostSubnet,
    TNonce nonce)
{
    static const auto Mask = TIP6Address::FromString("ffffffff:ffffffff:ffffffff:ffff0000");
    return TIP6Network(
        MakeMtnAddress(hostSubnet, 0, nonce),
        Mask);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NServer
} // namespace NYP

