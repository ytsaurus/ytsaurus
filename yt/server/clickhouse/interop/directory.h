#pragma once

#include "auth_token.h"

#include <util/generic/string.h>

#include <memory>
#include <vector>

namespace NInterop {

////////////////////////////////////////////////////////////////////////////////

/// Data model

using TNodeRevision = i64; // -1 means non-existing node

struct TChildNode
{
    TString Name;
    TString Content;

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
    TString Content;
};

////////////////////////////////////////////////////////////////////////////////

class IEphemeralNodeKeeper
{
public:
    virtual ~IEphemeralNodeKeeper() = default;

    virtual void Release() = 0;
};

using IEphemeralNodeKeeperPtr = std::shared_ptr<IEphemeralNodeKeeper>;

////////////////////////////////////////////////////////////////////////////////

class INodeEventHandler
{
public:
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

class IDirectory
{
public:
    virtual ~IDirectory() = default;

    virtual TString GetPath() const = 0;

    /// Synchronous accessors

    virtual TDirectoryListing ListNodes() = 0;

    virtual TNode GetNode(const TString& name) = 0;

    virtual bool NodeExists(const TString& name) = 0;

    /// TODO: Support documents?

    /// Ephemeral nodes

    virtual IEphemeralNodeKeeperPtr CreateAndKeepEphemeralNode(
        const TString& nameHint,
        const TString& content) = 0;

    /// One-shot subscriptions

    virtual void SubscribeToUpdate(
        TNodeRevision expectedRevision,
        INodeEventHandlerWeakPtr eventHandler) = 0;
};

using IDirectoryPtr = std::shared_ptr<IDirectory>;

////////////////////////////////////////////////////////////////////////////////

class ICoordinationService
{
public:
    virtual ~ICoordinationService() = default;

    virtual IAuthorizationTokenService* AuthTokenService() = 0;

    // Opens or creates a directory inside `path` associated with a current clique.
    virtual IDirectoryPtr OpenOrCreateDirectory(
        const IAuthorizationToken& authToken,
        const TString& path) = 0;
};

using ICoordinationServicePtr = std::shared_ptr<ICoordinationService>;

}   // namespace NInterop
