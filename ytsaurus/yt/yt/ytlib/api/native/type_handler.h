#pragma once

#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct ITypeHandler
    : public virtual TRefCounted
{
    virtual std::optional<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options) = 0;
    virtual std::optional<NCypressClient::TNodeId> CreateNode(
        NCypressClient::EObjectType type,
        const NYPath::TYPath& path,
        const TCreateNodeOptions& options) = 0;
    virtual std::optional<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options) = 0;
    virtual std::optional<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options) = 0;
    virtual std::optional<bool> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options) = 0;
    virtual std::optional<std::monostate> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options) = 0;
    virtual std::optional<std::monostate> AlterTableReplica(
        NTabletClient::TTableReplicaId replicaId,
        const TAlterTableReplicaOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITypeHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

