#pragma once

#include "common.h"
#include "node.h"

#include <util/generic/maybe.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

enum ENodeType
{
    NT_STRING,
    NT_INT64,
    NT_UINT64,
    NT_DOUBLE,
    NT_BOOLEAN,
    NT_MAP,
    NT_LIST,
    NT_FILE,
    NT_TABLE
};

struct TCreateOptions
{
    using TSelf = TCreateOptions;

    FLUENT_FIELD_DEFAULT(bool, Recursive, false);
    FLUENT_FIELD_DEFAULT(bool, IgnoreExisting, false);
    FLUENT_FIELD_OPTION(TNode, Attributes);
};

struct TRemoveOptions
{
    using TSelf = TRemoveOptions;

    FLUENT_FIELD_DEFAULT(bool, Recursive, false);
    FLUENT_FIELD_DEFAULT(bool, Force, false);
};

struct TGetOptions
{
    using TSelf = TGetOptions;

    FLUENT_FIELD_OPTION(TAttributeFilter, AttributeFilter);
    FLUENT_FIELD_OPTION(i64, MaxSize);
    FLUENT_FIELD_DEFAULT(bool, IgnoreOpaque, false);
};

struct TListOptions
{
    using TSelf = TListOptions;

    FLUENT_FIELD_OPTION(TAttributeFilter, AttributeFilter);
    FLUENT_FIELD_OPTION(i64, MaxSize);
};

struct TCopyOptions
{
    using TSelf = TCopyOptions;

    FLUENT_FIELD_DEFAULT(bool, Recursive, false);
    FLUENT_FIELD_DEFAULT(bool, PreserveAccount, false);
};

struct TMoveOptions
{
    using TSelf = TMoveOptions;

    FLUENT_FIELD_DEFAULT(bool, Recursive, false);
    FLUENT_FIELD_DEFAULT(bool, PreserveAccount, false);
};

struct TLinkOptions
{
    using TSelf = TLinkOptions;

    FLUENT_FIELD_DEFAULT(bool, Recursive, false);
    FLUENT_FIELD_DEFAULT(bool, IgnoreExisting, false);
    FLUENT_FIELD_OPTION(TNode, Attributes);
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
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
