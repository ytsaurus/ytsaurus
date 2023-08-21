#include "sequoia_service.h"

#include "bootstrap.h"
#include "path_resolver.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>
#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/ytlib/cypress_server/proto/sequoia_actions.pb.h>

#include <yt/yt/ytlib/sequoia_client/resolve_node.record.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/core/ytree/ypath_detail.h>
#include <yt/yt/core/ytree/ypath_service.h>

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaService
    : public TYPathServiceBase
{
public:
    TSequoiaService(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    TResolveResult Resolve(
        const TYPath& path,
        const IYPathServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Create);

        return TYPathServiceBase::DoInvoke(context);
    }

private:
    IBootstrap* const Bootstrap_;

    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create);
};

DEFINE_YPATH_SERVICE_METHOD(TSequoiaService, Create)
{
    auto Logger = CypressProxyLogger.WithTag("CypressRequestId: %v", context->GetRequestId());
    auto transaction = CreateSequoiaTransaction(
        Bootstrap_->GetNativeClient(),
        Logger);
    WaitFor(transaction->Start(/*options*/ {}))
        .ThrowOnError();

    auto path = GetRequestTargetYPath(context->RequestHeader());
    auto resolveResult = ResolvePath(transaction, path);
    if (std::holds_alternative<TCypressResolveResult>(resolveResult)) {
        THROW_ERROR_EXCEPTION(
            NObjectClient::EErrorCode::RequestInvolvesCypress,
            "Cypress request was passed to Sequoia");
    }
    YT_VERIFY(std::holds_alternative<TSequoiaResolveResult>(resolveResult));
    auto sequoiaResolveResult = std::get<TSequoiaResolveResult>(resolveResult);

    if (request->force()) {
        THROW_ERROR_EXCEPTION("Create with \"force\" flag is not supported in Sequoia");
    }
    if (request->recursive()) {
        THROW_ERROR_EXCEPTION("Create with \"recursive\" flag is not supported in Sequoia");
    }
    if (request->ignore_existing()) {
        THROW_ERROR_EXCEPTION("Create with \"ignore_existing\" flag is not supported in Sequoia");
    }
    if (request->ignore_type_mismatch()) {
        THROW_ERROR_EXCEPTION("Create with \"ignore_type_mismatch\" flag is not supported in Sequoia");
    }
    if (request->lock_existing()) {
        THROW_ERROR_EXCEPTION("Create with \"lock_existing\" flag is not supported in Sequoia");
    }
    if (GetTransactionId(context->RequestHeader())) {
        THROW_ERROR_EXCEPTION("Create with transaction is not supported in Sequoia");
    }

    auto parentNodeId = sequoiaResolveResult.ResolvedPrefixNodeId;
    auto cellTag = Bootstrap_->GetNativeConnection()->GetPrimaryMasterCellTag();
    if (CellTagFromId(parentNodeId) != cellTag) {
        THROW_ERROR_EXCEPTION("In Sequoia nodes can be created on primary cell only");
    }

    constexpr int TypicalPathTokenCount = 2;
    TCompactVector<TYPath, TypicalPathTokenCount> pathTokens;
    {
        NYPath::TTokenizer tokenizer(sequoiaResolveResult.UnresolvedSuffix);
        tokenizer.Advance();

        while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
            tokenizer.Expect(NYPath::ETokenType::Slash);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);
            pathTokens.push_back(TYPath(tokenizer.GetToken()));
            tokenizer.Advance();
        }
    }

    if (pathTokens.empty()) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::AlreadyExists,
            "%v already exists",
            path);
    }

    if (std::ssize(pathTokens) > 1) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::ResolveError,
            "Node %v has no child with key %Qv",
            sequoiaResolveResult.ResolvedPrefix,
            pathTokens[0]);
    }

    const auto& parentPath = sequoiaResolveResult.ResolvedPrefix;
    const auto& childKey = pathTokens[0];
    // NB: May differ from original path due to path rewrites.
    auto childPath = Format("%v/%v", parentPath, childKey);

    auto type = CheckedEnumCast<EObjectType>(request->type());

    // Generate new object id.
    auto childNodeId = transaction->GenerateObjectId(type, cellTag);

    // Acquire shared lock on parent node.
    NRecords::TResolveNodeKey parentKey{
        .Path = parentPath,
    };
    transaction->LockRow(parentKey, ELockType::SharedStrong);

    // Store new node in resolve table with exclusive lock.
    NRecords::TResolveNode childRecord{
        .Key = {
            .Path = childPath,
        },
        .NodeId = ToString(childNodeId),
    };
    transaction->WriteRow(childRecord);

    // Create new node on destination cell.
    NCypressServer::NProto::TReqCreateNode createNodeRequest;
    createNodeRequest.set_type(ToProto<int>(type));
    ToProto(createNodeRequest.mutable_node_id(), childNodeId);
    transaction->AddTransactionAction(cellTag, MakeTransactionActionData(createNodeRequest));

    // Attach new node to parent.
    NCypressServer::NProto::TReqAttachChild attachChildRequest;
    ToProto(attachChildRequest.mutable_parent_id(), parentNodeId);
    ToProto(attachChildRequest.mutable_child_id(), childNodeId);
    attachChildRequest.set_key(childKey);
    transaction->AddTransactionAction(cellTag, MakeTransactionActionData(attachChildRequest));

    TTransactionCommitOptions commitOptions{
        .CoordinatorCellId = Bootstrap_->GetNativeConnection()->GetPrimaryMasterCellId(),
        .Force2PC = true,
        .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
    };
    WaitFor(transaction->Commit(commitOptions))
        .ThrowOnError();

    ToProto(response->mutable_node_id(), childNodeId);
    response->set_cell_tag(ToProto<int>(cellTag));
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateSequoiaService(IBootstrap* bootstrap)
{
    return New<TSequoiaService>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
