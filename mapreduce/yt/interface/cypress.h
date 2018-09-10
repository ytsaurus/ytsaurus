#pragma once

#include "client_method_options.h"
#include "fwd.h"
#include "common.h"
#include "node.h"

#include <util/generic/maybe.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class ICypressClient
{
public:
    virtual TNodeId Create(
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options = TCreateOptions()) = 0;

    virtual void Remove(
        const TYPath& path,
        const TRemoveOptions& options = TRemoveOptions()) = 0;

    virtual bool Exists(
        const TYPath& path) = 0;

    virtual TNode Get(
        const TYPath& path,
        const TGetOptions& options = TGetOptions()) = 0;

    virtual void Set(
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options = TSetOptions()) = 0;

    virtual TNode::TListType List(
        const TYPath& path,
        const TListOptions& options = TListOptions()) = 0;

    virtual TNodeId Copy(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = TCopyOptions()) = 0;

    virtual TNodeId Move(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = TMoveOptions()) = 0;

    virtual TNodeId Link(
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = TLinkOptions()) = 0;

    virtual void Concatenate(
        const TVector<TYPath>& sourcePaths,
        const TYPath& destinationPath,
        const TConcatenateOptions& options = TConcatenateOptions()) = 0;

    virtual TRichYPath CanonizeYPath(const TRichYPath& path) = 0;

    virtual TVector<TTableColumnarStatistics> GetTableColumnarStatistics(const TVector<TRichYPath>& paths) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
