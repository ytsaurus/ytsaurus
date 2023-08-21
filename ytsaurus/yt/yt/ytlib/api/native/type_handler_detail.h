#pragma once

#include "type_handler.h"

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TNullTypeHandler
    : public ITypeHandler
{
public:
    std::optional<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options) override;
    std::optional<NCypressClient::TNodeId> CreateNode(
        NCypressClient::EObjectType type,
        const NYPath::TYPath& path,
        const TCreateNodeOptions& options) override;
    std::optional<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options) override;
    std::optional<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options) override;
    std::optional<bool> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options) override;
    std::optional<std::monostate> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options) override;
    std::optional<std::monostate> AlterTableReplica(
        NTabletClient::TTableReplicaId replicaId,
        const TAlterTableReplicaOptions& options) override;
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualTypeHandler
    : public TNullTypeHandler
{
public:
    std::optional<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options) override;
    std::optional<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options) override;
    std::optional<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options) override;
    std::optional<bool> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options) override;
    std::optional<std::monostate> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options) override;

protected:
    virtual NObjectClient::EObjectType GetSupportedObjectType() = 0;
    virtual NYson::TYsonString GetObjectYson(NObjectClient::TObjectId objectId) = 0;
    virtual std::optional<NObjectClient::TObjectId> DoCreateObject(
        const TCreateObjectOptions& options);
    virtual void DoRemoveObject(
        NObjectClient::TObjectId objectId,
        const TRemoveNodeOptions& options) = 0;

private:
    std::optional<NYson::TYsonString> TryGetObjectYson(
        const NYPath::TYPath& path,
        NYPath::TYPath* pathSuffix);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

