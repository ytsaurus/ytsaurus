#include "node_proxy.h"

#include "private.h"
#include "bootstrap.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/reverse_resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TRootstockProxy
    : public TYPathServiceBase
{
public:
    TRootstockProxy(
        IBootstrap* bootstrap,
        TYPath path,
        ISequoiaTransactionPtr transaction)
        : Bootstrap_(bootstrap)
        , Path_(std::move(path))
        , Transaction_(std::move(transaction))
    { }

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& /*context*/) override
    {
        // NB: For rootstock resolve cannot be performed on Сypress proxy, but the transaction has to be started there.
        // We assume path is correct, if this is not the case - prepare in 2PC will fail and the error will be propagated
        // to the user.
        return TResolveResultHere{path};
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        // NB: Only Create method is supported for rootstocks on Сypress proxy,
        // because technically rootstock is a Сypress object.
        DISPATCH_YPATH_SERVICE_METHOD(Create);

        return TYPathServiceBase::DoInvoke(context);
    }

private:
    IBootstrap* const Bootstrap_;
    const TYPath Path_;
    const ISequoiaTransactionPtr Transaction_;

    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create)
    {
        if (GetTransactionId(context->RequestHeader())) {
            THROW_ERROR_EXCEPTION("Rootstocks cannot be created in transaction");
        }

        auto type = CheckedEnumCast<EObjectType>(request->type());

        YT_VERIFY(type == EObjectType::Rootstock);

        const auto& connection = Bootstrap_->GetNativeConnection();
        const auto& rootstockCellTag = connection->GetPrimaryMasterCellTag();
        auto attributes = FromProto(request->node_attributes());
        auto scionCellTag = attributes->GetAndRemove<TCellTag>("scion_cell_tag");

        auto rootstockId = Transaction_->GenerateObjectId(type, rootstockCellTag, /*sequoia*/ false);
        auto scionId = Transaction_->GenerateObjectId(EObjectType::Scion, scionCellTag, /*sequoia*/ true);
        attributes->Set("scion_id", scionId);

        Transaction_->WriteRow(NRecords::TResolveNode{
            .Key = {.Path = MangleSequoiaPath(Path_)},
            .NodeId = scionId,
        });
        Transaction_->WriteRow(NRecords::TReverseResolveNode{
            .Key = {.NodeId = scionId},
            .Path = Path_,
        });

        NCypressClient::NProto::TReqCreateRootstock rootstockAction;
        rootstockAction.mutable_request()->CopyFrom(*request);
        rootstockAction.set_path(Path_);

        auto* createRootstockRequest = rootstockAction.mutable_request();
        ToProto(createRootstockRequest->mutable_hint_id(), rootstockId);
        ToProto(createRootstockRequest->mutable_node_attributes(), *attributes);

        Transaction_->AddTransactionAction(rootstockCellTag, MakeTransactionActionData(rootstockAction));

        auto rootstockCellId = connection->GetMasterCellId(rootstockCellTag);
        TTransactionCommitOptions commitOptions{
            .CoordinatorCellId = rootstockCellId,
            .Force2PC = true,
            .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
        };
        WaitFor(Transaction_->Commit(commitOptions))
            .ThrowOnError();

        // After transaction commit, scion creation request is posted via Hive,
        // so we sync secondary master with primary to make sure that scion is created.
        // Without this sync first requests to Sequoia subtree may fail because of scion
        // absence. Note that this is best effort since sync may fail.
        // TODO(h0pless): Rethink it when syncs for Sequoia transactions will be implemented.
        auto scionCellId = connection->GetMasterCellId(scionCellTag);
        WaitFor(connection->SyncHiveCellWithOthers({scionCellId}, rootstockCellId))
            .ThrowOnError();

        ToProto(response->mutable_node_id(), rootstockId);
        response->set_cell_tag(ToProto<int>(rootstockCellTag));
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IYPathServicePtr CreateRootstockProxy(
    IBootstrap* bootstrap,
    ISequoiaTransactionPtr transaction,
    TYPath resolvedPath)
{
    auto proxy = New<TRootstockProxy>(bootstrap, std::move(resolvedPath), std::move(transaction));
    return proxy;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
