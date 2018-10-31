#pragma once

#include "public.h"

#include "auth_token.h"

#include <yt/ytlib/api/native/public.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

/// Data model

using TNodeRevision = i64; // -1 means non-existing node

////////////////////////////////////////////////////////////////////////////////

struct TChildNode
{
    TString Name;
    THashMap<TString, TString> Attributes;

    bool operator <(const TChildNode& that) const
    {
        return Name < that.Name;
    }
};

using TChildNodeList = std::vector<TChildNode>;

struct TDirectoryListing
{
    TString Path;
    TChildNodeList Children;
    TNodeRevision Revision;
};

struct TNode
{
    TString Path;
    TNodeRevision Revision;
    THashMap<TString, TString> Attributes;
};

////////////////////////////////////////////////////////////////////////////////

struct IEphemeralNodeKeeper
{
    virtual ~IEphemeralNodeKeeper() = default;

    virtual void Release() = 0;
};

using IEphemeralNodeKeeperPtr = std::shared_ptr<IEphemeralNodeKeeper>;

////////////////////////////////////////////////////////////////////////////////

struct INodeEventHandler
{
    virtual ~INodeEventHandler() = default;

    virtual void OnUpdate(
        const TString& path,
        TNodeRevision newRevision) = 0;

    virtual void OnRemove(
        const TString& path) = 0;

    virtual void OnError(
        const TString& path,
        const TString& errorMessage) = 0;
};

using INodeEventHandlerPtr = std::shared_ptr<INodeEventHandler>;
using INodeEventHandlerWeakPtr = std::weak_ptr<INodeEventHandler>;

////////////////////////////////////////////////////////////////////////////////

struct IDirectory
{
    virtual ~IDirectory() = default;

    virtual TString GetPath() const = 0;

    /// Synchronous accessors

    virtual TDirectoryListing ListNodes() = 0;

    virtual TNode GetNode(const TString& name) = 0;

    virtual bool NodeExists(const TString& name) = 0;

    /// TODO: Support documents?

    /// Ephemeral nodes

    virtual IEphemeralNodeKeeperPtr CreateAndKeepEphemeralNode(
        const TString& name,
        const THashMap<TString, TString>& attributes) = 0;

    /// One-shot subscriptions

    virtual void SubscribeToUpdate(
        TNodeRevision expectedRevision,
        INodeEventHandlerWeakPtr eventHandler) = 0;
};

using IDirectoryPtr = std::shared_ptr<IDirectory>;

////////////////////////////////////////////////////////////////////////////////

struct ICoordinationService
{
    virtual ~ICoordinationService() = default;

    virtual IAuthorizationTokenService* AuthTokenService() = 0;

    // Opens or creates a directory inside `path` associated with a current clique.
    virtual IDirectoryPtr OpenOrCreateDirectory(
        const IAuthorizationToken& authToken,
        const TString& path) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICoordinationServicePtr CreateCoordinationService(
    NApi::NNative::IConnectionPtr connection,
    TString cliqueId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
