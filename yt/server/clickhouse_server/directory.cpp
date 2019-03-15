#include "directory.h"

#include "private.h"

#include "attributes_helpers.h"
#include "backoff.h"
#include "ephemeral_node.h"
#include "subscriptions.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/core/ytree/convert.h>

#include <util/generic/algorithm.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;

static const NLogging::TLogger& Logger = ServerLogger;

////////////////////////////////////////////////////////////////////////////////

static const TDuration DEFAULT_EPHEMERAL_NODE_TIMEOUT = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

class TDirectory
    : public TRefCounted
{
    using TSelf = TDirectory;

private:
    NApi::NNative::IClientPtr Client;
    TString Path;
    ISubscriptionManagerPtr SubscriptionManager;

public:
    TDirectory(
        NApi::NNative::IClientPtr client,
        TString path,
        ISubscriptionManagerPtr subscriptionManager)
        : Client(std::move(client))
        , Path(std::move(path))
        , SubscriptionManager(std::move(subscriptionManager))
    {}

    void CreateIfNotExists()
    {
        TCreateNodeOptions createOptions;
        createOptions.Recursive = true;
        createOptions.IgnoreExisting = true;

        WaitFor(Client->CreateNode(
            Path,
            NObjectClient::EObjectType::MapNode,
            createOptions))
            .ThrowOnError();
    }

    TDirectoryListing ListNodes()
    {
        YT_LOG_INFO("Listing nodes in coordination directory (Path: %v)", Path);

        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::Follower;
        options.SuppressAccessTracking = true;
        options.Attributes = {
            "key",
            "host",
            "tcp_port",
            "http_port",
            "revision",
        };

        auto result = WaitFor(Client->GetNode(Path, options));

        auto mapNode = ConvertToNode(result.ValueOrThrow());
        auto mapNodeRevision = GetAttribute<i64>(mapNode, "revision");

        auto children = mapNode->AsMap()->GetChildren();

        TDirectoryListing listing;
        listing.Path = Path;
        listing.Revision = mapNodeRevision;

        for (const auto& child : children) {
            auto childNode = child.second;
            const auto childName = GetAttribute<TString>(childNode, "key");
            auto* attributes = childNode->MutableAttributes();
            attributes->Remove("revision");
            YT_LOG_DEBUG("Node listed (Path: %v, Name: %v, Attributes: %v)", Path, childName, NYTree::ConvertToYsonString(attributes, NYson::EYsonFormat::Text));
            listing.Children.push_back(TChildNode{
                .Name = childName,
                .Attributes = ConvertTo<THashMap<TString, TString>>(*attributes)
            });
        }

        Sort(listing.Children.begin(), listing.Children.end());

        return listing;
    }

    IEphemeralNodeKeeperPtr CreateAndKeepEphemeralNode(
        const TString& nameHint,
        const THashMap<TString, TString>& attributes)
    {
        return CreateEphemeralNodeKeeper(
            Client,
            Path,
            nameHint,
            attributes,
            DEFAULT_EPHEMERAL_NODE_TIMEOUT);
    }

    void SubscribeToUpdate(
        TNodeRevision expectedRevision,
        INodeEventHandlerWeakPtr eventHandler)
    {
        return SubscriptionManager->Subscribe(
            Client,
            Path,
            expectedRevision,
            eventHandler);
    }

private:
    void ValidateChildName(const TString& name) const
    {
        if (name.find('/') != TString::npos) {
            THROW_ERROR_EXCEPTION("Path component separator found in child node name")
                << TErrorAttribute("name", name);
        }
    }
};

DECLARE_REFCOUNTED_CLASS(TDirectory);
DEFINE_REFCOUNTED_TYPE(TDirectory);

////////////////////////////////////////////////////////////////////////////////

class TDirectorySyncWrapper
    : public IDirectory
{
private:
    TString Path;
    TDirectoryPtr Impl;

public:
    TDirectorySyncWrapper(
        TString path,
        TDirectoryPtr directory)
        : Path(std::move(path))
        , Impl(std::move(directory))
    {
    }

    TString GetPath() const override
    {
        return Path;
    }

    TDirectoryListing ListNodes() override
    {
        return Impl->ListNodes();
    }

    TNode GetNode(const TString& /* name */) override
    {
        YCHECK(false);
    }

    bool NodeExists(const TString& /* name */) override
    {
        YCHECK(false);
    }

    IEphemeralNodeKeeperPtr CreateAndKeepEphemeralNode(
        const TString& nameHint,
        const THashMap<TString, TString>& attributes) override
    {
        return Impl->CreateAndKeepEphemeralNode(nameHint, attributes);
    }

    void SubscribeToUpdate(
        TNodeRevision expectedRevision,
        INodeEventHandlerWeakPtr eventHandler) override
    {
        return Impl->SubscribeToUpdate(expectedRevision, eventHandler);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCoordinationService
    : public ICoordinationService
{
private:
    NApi::NNative::IClientPtr Client_;
    ISubscriptionManagerPtr SubscriptionManager_;
    TString CliqueId_;

public:
    TCoordinationService(NApi::NNative::IClientPtr client, ISubscriptionManagerPtr subscriptionManager, TString cliqueId)
        : Client_(std::move(client))
        , SubscriptionManager_(std::move(subscriptionManager))
        , CliqueId_(cliqueId)
    { }

    IDirectoryPtr OpenOrCreateDirectory(const TString& path) override
    {
        auto directory = New<TDirectory>(Client_, path + "/" + CliqueId_, SubscriptionManager_);

        directory->CreateIfNotExists();

        return std::make_shared<TDirectorySyncWrapper>(path, std::move(directory));
    }
};

////////////////////////////////////////////////////////////////////////////////

ICoordinationServicePtr CreateCoordinationService(NApi::NNative::IClientPtr rootClient, TString cliqueId)
{
    auto subscriptionManager = CreateSubscriptionManager();
    return std::make_shared<TCoordinationService>(std::move(rootClient), std::move(subscriptionManager), std::move(cliqueId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
