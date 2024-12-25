#pragma once

#include "public.h"

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaResolveIterationResult
{
    NCypressClient::TNodeId Id;
    NSequoiaClient::TAbsoluteYPath Path;
};

struct TSequoiaResolveResult
{
    NCypressClient::TNodeId Id;
    NSequoiaClient::TAbsoluteYPath Path;
    NSequoiaClient::TYPath UnresolvedSuffix;
    //! May be null for resolved scion or snapshot branch.
    NCypressClient::TNodeId ParentId;

    bool IsSnapshot() const noexcept;
};

struct TCypressResolveResult
{
    //! Cypress path which should be resolved by master.
    NSequoiaClient::TRawYPath Path;
};

//! Resolves path via Sequoia tables. If path is not resolved returns
//! |TCypressResolveResult|. Takes optional output parameter #history. History
//! consists of encountered links.
TResolveResult ResolvePath(
    const TSequoiaSessionPtr& session,
    NSequoiaClient::TRawYPath rawPath,
    TStringBuf method,
    std::vector<TSequoiaResolveIterationResult>* history = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
