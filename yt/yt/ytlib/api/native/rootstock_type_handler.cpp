#include "rootstock_type_handler.h"

#include "type_handler_detail.h"
#include "client_impl.h"

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/sequoia_client/resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TRootstockTypeHandler
    : public TNullTypeHandler
{
public:
    explicit TRootstockTypeHandler(TClient* client)
        : Client_(client)
    { }

    std::optional<TObjectId> CreateNode(
        EObjectType type,
        const TYPath& path,
        const TCreateNodeOptions& options) override
    {
        if (type != EObjectType::Rootstock) {
            return {};
        }

        if (options.TransactionId) {
            THROW_ERROR_EXCEPTION("Rootstocks cannot be created in transaction");
        }

        auto transaction = CreateSequoiaTransaction(Client_, ApiLogger);
        WaitFor(transaction->Start(/*options*/ {}))
            .ThrowOnError();

        const auto& connection = Client_->GetNativeConnection();
        auto rootstockCellTag = connection->GetPrimaryMasterCellTag();
        auto rootstockCellId = connection->GetPrimaryMasterCellId();

        auto attributes = options.Attributes ? options.Attributes : EmptyAttributes().Clone();
        auto scionCellTag = attributes->GetAndRemove<TCellTag>("scion_cell_tag");
        auto scionCellId = connection->GetMasterCellId(scionCellTag);

        auto rootstockId = transaction->GenerateObjectId(
            EObjectType::Rootstock,
            rootstockCellTag,
            /*sequoia*/ false);
        // NB: Rootstock is not a sequoia object.
        YT_VERIFY(!IsSequoiaId(rootstockId));

        auto scionId = transaction->GenerateObjectId(EObjectType::Scion, scionCellTag);
        attributes->Set("scion_id", scionId);

        NRecords::TResolveNode record{
            .Key = {
                .Path = path,
            },
            .NodeId = ToString(scionId),
        };
        transaction->WriteRow(record);

        NCypressClient::NProto::TReqCreateRootstock rootstockAction;
        auto* request = rootstockAction.mutable_request();
        request->set_type(ToProto<int>(EObjectType::Rootstock));
        request->set_recursive(options.Recursive);
        ToProto(request->mutable_hint_id(), rootstockId);

        if (options.IgnoreExisting) {
            THROW_ERROR_EXCEPTION(
                "\"ignore_existing\" option is not supported for rootstock creation");
        } else {
            request->set_ignore_existing(false);
        }

        request->set_lock_existing(options.LockExisting);
        request->set_force(options.Force);
        request->set_ignore_type_mismatch(options.IgnoreTypeMismatch);
        ToProto(request->mutable_node_attributes(), *attributes);

        rootstockAction.set_path(path);
        transaction->AddTransactionAction(rootstockCellTag, MakeTransactionActionData(rootstockAction));

        TTransactionCommitOptions commitOptions{
            .CoordinatorCellId = rootstockCellId,
            .Force2PC = true,
            .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
        };
        WaitFor(transaction->Commit(commitOptions))
            .ThrowOnError();

        // After transaction commit, scion creation request is posted via Hive,
        // so we sync secondary master with primary to make sure that scion is created.
        // Without this sync first requests to Sequoia subtree may fail because of scion
        // absence. Note that this is best effort since sync may fail.
        // TODO: Rethink it when syncs for Sequoia transactions will be implemented.
        WaitFor(connection->SyncHiveCellWithOthers({scionCellId}, rootstockCellId))
            .ThrowOnError();

        return rootstockId;
    }

private:
    TClient* const Client_;
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateRootstockTypeHandler(TClient* client)
{
    return New<TRootstockTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

