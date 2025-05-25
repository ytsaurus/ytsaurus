#ifndef NODE_PROXY_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include node_proxy_base.h"
// For the sake of sane code completion.
#include "node_proxy_base.h"
#endif

#include "action_helpers.h"
#include "bootstrap.h"
#include "response_keeper.h"
#include "sequoia_session.h"
#include "config.h"

#include <yt/yt/ytlib/sequoia_client/transaction.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

template<class TCtxPtr>
void TNodeProxyBase::FinishSequoiaSessionAndReply(
    const TCtxPtr& context,
    NObjectClient::TCellId coordinatorCellId,
    bool commitSession)
{
    auto responseMessage = CreateResponseMessage(context->Response(), context->Response().Attachments());

    auto mutationId = context->GetMutationId();
    const auto& responseKeeper = Bootstrap_->GetResponseKeeper();
    if (commitSession && mutationId) {
        responseKeeper->KeepResponse(SequoiaSession_->SequoiaTransaction(), mutationId, responseMessage);
    }

    // TODO(cherepashka): after `set` is done, make all responses with mutationId handled correctly.
    if (commitSession) {
        try {
            SequoiaSession_->Commit(coordinatorCellId);
        } catch (const std::exception& ex) {
            auto useResponseKeeper = mutationId && responseKeeper->GetDynamicConfig()->Enable;
            if (useResponseKeeper) {
                // NB: If commit failed, then some error related to dynamic tables occured and client will receive it,
                // so we should save this error in case of retries on client side.
                auto sequoiaTransaction = NConcurrency::WaitFor(
                    StartCypressProxyTransaction(
                        Bootstrap_->GetSequoiaClient(),
                        NSequoiaClient::ESequoiaTransactionType::ResponseKeeper)
                    ).ValueOrThrow();
                responseKeeper->KeepResponse(sequoiaTransaction, mutationId, TError(ex));
                NConcurrency::WaitFor(sequoiaTransaction->Commit({
                    .CoordinatorCellId = coordinatorCellId,
                    .CoordinatorPrepareMode = NApi::ETransactionCoordinatorPrepareMode::Late,
                }))
                    .ThrowOnError();
            }
            THROW_ERROR_EXCEPTION(ex);
        }
    } else {
        SequoiaSession_->Abort();
    }

    context->Reply(responseMessage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
