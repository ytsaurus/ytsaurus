#pragma once

#include "client_method_options.h"
#include "fwd.h"
#include "common.h"
#include "node.h"

#include <util/generic/maybe.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

enum ENodeType : int
{
    NT_STRING               /* "string_node" */,
    NT_INT64                /* "int64_node" */,
    NT_UINT64               /* "uint64_node" */,
    NT_DOUBLE               /* "double_node" */,
    NT_BOOLEAN              /* "boolean_node" */,
    NT_MAP                  /* "map_node" */,
    NT_LIST                 /* "list_node" */,
    NT_FILE                 /* "file" */,
    NT_TABLE                /* "table" */,
    NT_DOCUMENT             /* "document" */,
    NT_REPLICATED_TABLE     /* "replicated_table" */,
    NT_TABLE_REPLICA        /* "table_replica" */,
};

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
        const TNode& value) = 0;

    virtual TNode::TList List(
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
        const yvector<TYPath>& sourcePaths,
        const TYPath& destinationPath,
        const TConcatenateOptions& options = TConcatenateOptions()) = 0;

    virtual TRichYPath CanonizeYPath(const TRichYPath& path) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
