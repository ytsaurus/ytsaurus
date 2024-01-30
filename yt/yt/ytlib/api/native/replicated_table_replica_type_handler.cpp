#include "replicated_table_replica_type_handler.h"

#include "type_handler_detail.h"
#include "client_impl.h"
#include "ypath_helpers.h"

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/tablet_client/helpers.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYPath;
using namespace NYTree;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

class TReplicatedTableReplicaTypeHandler
    : public TNullTypeHandler
{
public:
    explicit TReplicatedTableReplicaTypeHandler(TClient* client)
        : Client_(client)
    { }

    virtual std::optional<TObjectId> CreateObject(
        EObjectType type,
        const TCreateObjectOptions& options) override
    {
        if (type != EObjectType::TableReplica) {
            return {};
        }

        auto attributes = options.Attributes ? options.Attributes->Clone() : EmptyAttributes().Clone();

        TCellTag cellTag;

        {
            TTableId tableId;
            auto tablePath = attributes->Get<TString>("table_path");
            Client_->ValidatePermissionImpl(tablePath, EPermission::Write);
            ResolveExternalTable(Client_, tablePath, &tableId, &cellTag);
            attributes->Set("table_path", FromObjectId(tableId));
        }

        {
            auto clusterName = attributes->Get<TString>("cluster_name");
            auto result = WaitFor(Client_->NodeExists(GetCypressClusterPath(clusterName), {}));
            THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error checking replica cluster existence");
            if (!result.Value()) {
                THROW_ERROR_EXCEPTION("Replica cluster %Qv does not exist", clusterName);
            }
        }

        return Client_->CreateObjectImpl(
            type,
            cellTag,
            *attributes,
            options);
    }

    std::optional<TYsonString> GetNode(
        const TYPath& path,
        const TGetNodeOptions& /*options*/) override
    {
        MaybeValidatePermission(path, EPermission::Read);
        return {};
    }

    std::optional<TYsonString> ListNode(
        const TYPath& path,
        const TListNodeOptions& /*options*/) override
    {
        MaybeValidatePermission(path, EPermission::Read);
        return {};
    }

    std::optional<bool> NodeExists(
        const TYPath& path,
        const TNodeExistsOptions& /*options*/) override
    {
        try {
            MaybeValidatePermission(path, EPermission::Read);
        } catch (const std::exception& ex) {
            if (!TError(ex).FindMatching(NYTree::EErrorCode::ResolveError)) {
                throw;
            }
        }
        return {};
    }

    std::optional<std::monostate> RemoveNode(
        const TYPath& path,
        const TRemoveNodeOptions& /*options*/) override
    {
        MaybeValidatePermission(path, EPermission::Write);
        return {};
    }

    std::optional<std::monostate> AlterTableReplica(
        TTableReplicaId replicaId,
        const TAlterTableReplicaOptions& options) override
    {
        if (TypeFromId(replicaId) != EObjectType::TableReplica) {
            return {};
        }

        Client_->ValidateTableReplicaPermission(replicaId, EPermission::Write);

        auto req = TTableReplicaYPathProxy::Alter(FromObjectId(replicaId));
        Client_->SetMutationId(req, options);
        if (options.Enabled) {
            req->set_enabled(*options.Enabled);
        }
        if (options.Mode) {
            if (!IsStableReplicaMode(*options.Mode)) {
                THROW_ERROR_EXCEPTION("Invalid replica mode %Qlv", *options.Mode);
            }
            req->set_mode(static_cast<int>(*options.Mode));
        }
        if (options.PreserveTimestamps) {
            req->set_preserve_timestamps(*options.PreserveTimestamps);
        }
        if (options.Atomicity) {
            req->set_atomicity(static_cast<int>(*options.Atomicity));
        }
        if (options.EnableReplicatedTableTracker) {
            req->set_enable_replicated_table_tracker(*options.EnableReplicatedTableTracker);
        }

        auto cellTag = CellTagFromId(replicaId);
        auto proxy = Client_->CreateObjectServiceWriteProxy(cellTag);
        WaitFor(proxy.Execute(req))
            .ThrowOnError();

        return std::monostate();
    }

private:
    TClient* const Client_;


    void MaybeValidatePermission(const TYPath& path, EPermission permission)
    {
        TObjectId objectId;
        if (!TryParseObjectId(path, &objectId) || TypeFromId(objectId) != EObjectType::TableReplica) {
            return;
        }

        Client_->ValidateTableReplicaPermission(objectId, permission, /*options*/ {});
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateReplicatedTableReplicaTypeHandler(TClient* client)
{
    return New<TReplicatedTableReplicaTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
