#ifndef NODE_PROXY_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include node_proxy_base.h"
// For the sake of sane code completion.
#include "node_proxy_base.h"
#endif

#include "bootstrap.h"
#include "response_keeper.h"
#include "sequoia_session.h"

#include <yt/yt/server/lib/cypress_proxy/config.h>

#include <yt/yt/ytlib/sequoia_client/transaction.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

template <class TCtxPtr>
void TNodeProxyBase::FinishSequoiaSessionAndReply(
    const TCtxPtr& context,
    NObjectClient::TCellId coordinatorCellId,
    bool commitSession)
{
    auto responseMessage = CreateResponseMessage(context->Response(), context->Response().Attachments());

    if (commitSession) {
        if (auto mutationId = context->GetMutationId()) {
            const auto& responseKeeper = Bootstrap_->GetResponseKeeper();
            responseKeeper->KeepResponse(SequoiaSession_->SequoiaTransaction(), mutationId, responseMessage);
        }
        SequoiaSession_->Commit(coordinatorCellId);
    } else {
        SequoiaSession_->Abort();
    }

    context->Reply(responseMessage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
