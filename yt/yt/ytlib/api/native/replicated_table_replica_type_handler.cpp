#include "default_type_handler.h"

#include "type_handler.h"
#include "client_impl.h"
#include "ypath_helpers.h"

#include <yt/yt/client/object_client/helpers.h>

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
    : public ITypeHandler
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
            Client_->ResolveExternalTable(tablePath, &tableId, &cellTag);
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
        MaybeValidatePermission(path);
        return {};
    }

    std::optional<TYsonString> ListNode(
        const TYPath& path,
        const TListNodeOptions& /*options*/) override
    {
        MaybeValidatePermission(path);
        return {};
    }

private:
    TClient* const Client_;


    void MaybeValidatePermission(const TYPath& path)
    {
        TObjectId objectId;
        if (!TryParseObjectId(path, &objectId) || TypeFromId(objectId) != EObjectType::TableReplica) {
            return;
        }

        Client_->ValidateTableReplicaPermission(objectId, EPermission::Read, /*options*/ {});
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateReplicatedTableReplicaTypeHandler(TClient* client)
{
    return New<TReplicatedTableReplicaTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
