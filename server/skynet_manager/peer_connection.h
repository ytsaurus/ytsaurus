#pragma once

#include "public.h"

#include <yt/core/net/public.h>
#include <yt/core/ytree/public.h>
#include <yt/core/logging/log.h>
#include <yt/core/misc/guid.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

struct THandshake
{
    TString PeerId;
    TString PeerName;
    TResourceId ResourceId;
};

DEFINE_ENUM(EPeerMessage,
    (WantResource)
    (HasResource)
    (Ping)
    (Unknown)
);

////////////////////////////////////////////////////////////////////////////////

class TPeerConnection
    : public TRefCounted
{
public:
    TPeerConnection(const NNet::IConnectionPtr& connection);

    void SendHandshake(const THandshake& handshake);
    void SendPing();
    void SendHasResource();
    void SendResource(const NYTree::INodePtr& node);
    void SendLinks(const NYTree::INodePtr& node);

    THandshake ReceiveHandshake();
    EPeerMessage ReceiveMessage();

    const NNet::TNetworkAddress& PeerAddress() const;

private:
    const TGuid ConnectionId_;
    NLogging::TLogger Logger;
    const NNet::IConnectionPtr Connection_;

    std::unique_ptr<IInputStream> Input_;
    std::unique_ptr<IOutputStream> Output_;

    std::pair<TString, NYTree::INodePtr> ReceiveRawMessage();
    void SendRawMessage(const TString& type, const NYTree::INodePtr& msg);
};

DEFINE_REFCOUNTED_TYPE(TPeerConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
