#pragma once

#include "private.h"
#include "node_proxy.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

class TNodeProxyBase
    : public INodeProxy
{
public:
    EInvokeResult Invoke(const ISequoiaServiceContextPtr& context) override;

protected:
    IBootstrap* const Bootstrap_;
    const TSequoiaSessionPtr SequoiaSession_;
    const NApi::NNative::IClientPtr NativeAuthenticatedClient_;

    // NB: If this field is set to #ForwardToMaster, no change in persistent state should
    // be made, and vice versa.
    //! Most requests are executed on Cypress proxy, but there is a chance, that
    //! during the execution of a verb, it becomes apparent, that the request
    //! has to be handeled by master. This override exists for such cases.
    EInvokeResult InvokeResult_ = EInvokeResult::Executed;

    TNodeProxyBase(IBootstrap* bootstrap, TSequoiaSessionPtr sequoiaSession);

    virtual void BeforeInvoke(const ISequoiaServiceContextPtr& context);
    virtual bool DoInvoke(const ISequoiaServiceContextPtr& context);
    virtual void AfterInvoke(const ISequoiaServiceContextPtr& context);

    NObjectClient::TCellId CellIdFromCellTag(NObjectClient::TCellTag cellTag) const;
    NObjectClient::TCellId CellIdFromObjectId(NObjectClient::TObjectId id);

    //! Constructs the response message, saves it into Sequoia response keeper if
    //! needed, commits sequoia session and replies with constructed message.
    template <class TCtxPtr>
    void FinishSequoiaSessionAndReply(
        const TCtxPtr& context,
        NObjectClient::TCellId coordinatorCellId,
        bool commitSession);

    const NApi::NNative::IClientPtr& GetNativeAuthenticatedClient() const;
};

DEFINE_REFCOUNTED_TYPE(TNodeProxyBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy

#define NODE_PROXY_BASE_INL_H_
#include "node_proxy_base-inl.h"
#undef NODE_PROXY_BASE_INL_H_
