#pragma once

#include "public.h"

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

static const auto AmpersandYPath = NYPath::TYPath("&");

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaResolveIterationResult
{
    NCypressClient::TNodeId Id;
    NSequoiaClient::TAbsolutePath Path;
};

struct TSequoiaResolveResult
{
    NCypressClient::TNodeId Id;
    NSequoiaClient::TAbsolutePath Path;
    NYPath::TYPath UnresolvedSuffix;
    //! May be null for resolved scion or snapshot branch.
    NCypressClient::TNodeId ParentId;
    //! A sequence representing the full resolution path. In most cases,
    //! it starts from the scion and ends with the resolved node itself.
    //! For snapshot branches, the path may be trivial (e.g., contain
    //! only the resolved node).
    std::vector<TCypressNodeDescriptor> NodeAncestry;

    bool IsSnapshot() const noexcept;
};

struct TMasterResolveResult
{ };

struct TCypressResolveResult
{
    //! Cypress path which should be resolved by master.
    NYPath::TYPath Path;
};

struct TUnreachableSequoiaResolveResult
{
    NCypressClient::TNodeId Id;
};

//! Resolves path via Sequoia tables. If path is not resolved returns
//! |TCypressResolveResult|. Takes optional output parameter #history. History
//! consists of encountered links.
TResolveResult ResolvePath(
    const TSequoiaSessionPtr& session,
    NYPath::TYPath path,
    bool pathIsAdditional,
    TStringBuf service,
    TStringBuf method,
    std::vector<TSequoiaResolveIterationResult>* history = nullptr);

//! The same as ResolvePath() but returns unreachable Sequoia resolve result if
//! target object ID was not found in resolve table. Used only to allow using
//! CheckPermission for orphaned nodes without transaction.
TMaybeUnreachableResolveResult ResolvePathWithUnreachableResultAllowed(
    const TSequoiaSessionPtr& session,
    NYPath::TYPath path,
    TStringBuf service,
    TStringBuf method);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
