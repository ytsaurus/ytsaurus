#include "directory.h"

#include "private.h"

#include "attributes_helpers.h"
#include "auth_token.h"
#include "backoff.h"
#include "ephemeral_node.h"
#include "subscriptions.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/core/ytree/convert.h>

#include <util/generic/algorithm.h>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NYTree;

static const NLogging::TLogger& Logger = ServerLogger;

////////////////////////////////////////////////////////////////////////////////

static const TDuration DEFAULT_EPHEMERAL_NODE_TIMEOUT = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

class TDirectory
    : public TRefCounted
{
    using TSelf = TDirectory;

private:
    IInvokerPtr Invoker;
    NNative::IClientPtr Client;
    TString Path;
    ISubscriptionManagerPtr SubscriptionManager;

public:
    TDirectory(
        NNative::IClientPtr client,
        TString path,
        ISubscriptionManagerPtr subscriptionManager)
        : Invoker(client->GetNativeConnection()->GetInvoker())
        , Client(std::move(client))
        , Path(std::move(path))
        , SubscriptionManager(std::move(subscriptionManager))
    {}

    TFuture<void> CreateIfNotExists()
    {
       return BIND(&TSelf::DoCreateIfNotExists, MakeStrong(this))
            .AsyncVia(Invoker)
            .Run();
    }

    TFuture<NInterop::TDirectoryListing> ListNodes()
    {
        return BIND(&TSelf::DoListNodes, MakeStrong(this))
            .AsyncVia(Invoker)
            .Run();
    }

    /* TFuture<NInterop::TNode> GetNode(const TString& name)
    {
        return BIND(&TSelf::DoGetNode, MakeStrong(this))
            .AsyncVia(Invoker)
            .Run(name);
    }

    TFuture<bool> NodeExists(const TString& name)
    {
        return BIND(&TSelf::DoNodeExists, MakeStrong(this))
            .AsyncVia(Invoker)
            .Run(name);
    } */

    TFuture<NInterop::IEphemeralNodeKeeperPtr> CreateAndKeepEphemeralNode(
        const TString& nameHint,
        const THashMap<TString, TString>& attributes)
    {
        return BIND(&TSelf::DoCreateAndKeepEphemeralNode, MakeStrong(this))
            .AsyncVia(Invoker)
            .Run(nameHint, attributes);
    }

    TFuture<void> SubscribeToUpdate(
        NInterop::TNodeRevision expectedRevision,
        NInterop::INodeEventHandlerWeakPtr eventHandler)
    {
        return BIND(&TSelf::DoSubscribeToUpdate, MakeStrong(this))
            .AsyncVia(Invoker)
            .Run(expectedRevision, eventHandler);
    }

private:
    TString GetChildNodePath(const TString& name) const;
    void ValidateChildName(const TString& name) const;

    void DoCreateIfNotExists();

    NInterop::TDirectoryListing DoListNodes();

    NInterop::TNode DoGetNode(const TString& name);

    bool DoNodeExists(const TString& name);

    NInterop::IEphemeralNodeKeeperPtr DoCreateAndKeepEphemeralNode(
        const TString& nameHint,
        const THashMap<TString, TString>& attributes);

    void DoSubscribeToUpdate(
        NInterop::TNodeRevision expectedRevision,
        NInterop::INodeEventHandlerWeakPtr eventHandler);
};

DECLARE_REFCOUNTED_CLASS(TDirectory);
DEFINE_REFCOUNTED_TYPE(TDirectory);

////////////////////////////////////////////////////////////////////////////////

TString TDirectory::GetChildNodePath(const TString& name) const
{
    ValidateChildName(name);
    return TString::Join(Path, '/', name);
}

void TDirectory::ValidateChildName(const TString& name) const
{
    if (name.find('/') != TString::npos) {
        THROW_ERROR_EXCEPTION("Path component separator found in child node name")
            << TErrorAttribute("name", name);
    }
}

void TDirectory::DoCreateIfNotExists()
{
    TCreateNodeOptions createOptions;
    createOptions.Recursive = true;
    createOptions.IgnoreExisting = true;

    auto result = WaitFor(Client->CreateNode(
        Path,
        NObjectClient::EObjectType::MapNode,
        createOptions));

    result.ThrowOnError();
}

NInterop::TDirectoryListing TDirectory::DoListNodes()
{
    LOG_INFO("Listing nodes in coordination directory (Path: %v)", Path);

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

    NInterop::TDirectoryListing listing;
    listing.Path = Path;
    listing.Revision = mapNodeRevision;

    for (const auto& child : children) {
        auto childNode = child.second;
        const auto childName = GetAttribute<TString>(childNode, "key");
        auto* attributes = childNode->MutableAttributes();
        attributes->Remove("revision");
        LOG_DEBUG("Node listed (Path: %v, Name: %v, Attributes: %v)", Path, childName, NYTree::ConvertToYsonString(attributes, NYson::EYsonFormat::Text));
        listing.Children.push_back(NInterop::TChildNode{
            .Name = childName,
            .Attributes = ConvertTo<THashMap<TString, TString>>(*attributes)
        });
    }

    Sort(listing.Children.begin(), listing.Children.end());

    return listing;
}

/* NInterop::TNode TDirectory::DoGetNode(const TString& name)
{
    LOG_INFO("Reading child node %Qlv in coordination directory %Qlv", name, Path);

    TGetNodeOptions options;
    options.SuppressAccessTracking = true;
    options.ReadFrom = EMasterChannelKind::Follower;
    options.Attributes = {
        "revision",
    };

    auto path = GetChildNodePath(name);

    const auto result = WaitFor(Client->GetNode(path, options));
    const auto node = ConvertToNode(result.ValueOrThrow());
    const auto revision = GetAttribute<i64>(node, "revision");
    const auto content = node->AsString()->GetValue();

    LOG_DEBUG("Get node %Qv, content = %Qv, revision = %v", path, content, revision);

    return NInterop::TNode{
        .Path = path,
        .Revision = revision,
        .Content = content};
}

bool TDirectory::DoNodeExists(const TString& name)
{
    LOG_INFO("Checking is node exists in coordination directory %Qv", Path);

    TNodeExistsOptions options;
    options.ReadFrom = EMasterChannelKind::Follower;
    options.SuppressAccessTracking = true;

    return WaitFor(Client->NodeExists(GetChildNodePath(name), options))
        .ValueOrThrow();
} */

NInterop::IEphemeralNodeKeeperPtr TDirectory::DoCreateAndKeepEphemeralNode(
    const TString& name,
    const THashMap<TString, TString>& attributes)
{
    return CreateEphemeralNodeKeeper(
        Client,
        Path,
        name,
        attributes,
        DEFAULT_EPHEMERAL_NODE_TIMEOUT);
}

void TDirectory::DoSubscribeToUpdate(
    NInterop::TNodeRevision expectedRevision,
    NInterop::INodeEventHandlerWeakPtr eventHandler)
{
    return SubscriptionManager->Subscribe(
        Client,
        Path,
        expectedRevision,
        eventHandler);
}

////////////////////////////////////////////////////////////////////////////////

class TDirectorySyncWrapper
    : public NInterop::IDirectory
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

    NInterop::TDirectoryListing ListNodes() override
    {
        return WaitFor(Impl->ListNodes())
            .ValueOrThrow();
    }

    NInterop::TNode GetNode(const TString& /* name */) override
    {
        YCHECK(false);
        /* return WaitFor(Impl->GetNode(name))
            .ValueOrThrow(); */
    }

    bool NodeExists(const TString& /* name */) override
    {
        YCHECK(false);
        /* return WaitFor(Impl->NodeExists(name))
            .ValueOrThrow(); */
    }

    NInterop::IEphemeralNodeKeeperPtr CreateAndKeepEphemeralNode(
        const TString& nameHint,
        const THashMap<TString, TString>& attributes) override
    {
        return WaitFor(Impl->CreateAndKeepEphemeralNode(nameHint, attributes))
            .ValueOrThrow();
    }

    void SubscribeToUpdate(
        NInterop::TNodeRevision expectedRevision,
        NInterop::INodeEventHandlerWeakPtr eventHandler) override
    {
        return WaitFor(Impl->SubscribeToUpdate(expectedRevision, eventHandler))
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCoordinationService
    : public NInterop::ICoordinationService
{
private:
    NNative::IConnectionPtr Connection;
    ISubscriptionManagerPtr SubscriptionManager;
    TString CliqueId_;

public:
    TCoordinationService(
        NNative::IConnectionPtr client,
        ISubscriptionManagerPtr subscriptionManager,
        TString cliqueId);

    NInterop::IAuthorizationTokenService* AuthTokenService() override
    {
        return GetAuthTokenService();
    }

    NInterop::IDirectoryPtr OpenOrCreateDirectory(
        const NInterop::IAuthorizationToken& token,
        const TString& path) override;
};

////////////////////////////////////////////////////////////////////////////////

TCoordinationService::TCoordinationService(
    NNative::IConnectionPtr connection,
    ISubscriptionManagerPtr subscriptionManager,
    TString cliqueId)
    : Connection(std::move(connection))
    , SubscriptionManager(std::move(subscriptionManager))
    , CliqueId_(cliqueId)
{}

NInterop::IDirectoryPtr TCoordinationService::OpenOrCreateDirectory(
    const NInterop::IAuthorizationToken& authToken,
    const TString& path)
{
    auto client = Connection->CreateNativeClient(UnwrapAuthToken(authToken));
    auto directory = New<TDirectory>(std::move(client), path + "/" + CliqueId_, SubscriptionManager);

    WaitFor(directory->CreateIfNotExists())
       .ThrowOnError();

    return std::make_shared<TDirectorySyncWrapper>(
        path,
        std::move(directory));
}

////////////////////////////////////////////////////////////////////////////////

NInterop::ICoordinationServicePtr CreateCoordinationService(
    NApi::NNative::IConnectionPtr connection,
    TString cliqueId)
{
    auto subscriptionManager = CreateSubscriptionManager();

    return std::make_shared<TCoordinationService>(
        std::move(connection),
        std::move(subscriptionManager),
        std::move(cliqueId));
}

}   // namespace NClickHouse
}   // namespace NYT
