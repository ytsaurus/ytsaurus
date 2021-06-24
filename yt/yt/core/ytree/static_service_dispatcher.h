#pragma once

#include "ypath_detail.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Helper for organizing several YPath services into a map-like object.
/*!
 *  This helper is designed for a small number of YPath services with well-known names.
 *  To handle arbitrary size maps of similar objects see |TVirtualMapBase|.
 */
class TStaticServiceDispatcher
    : public virtual TYPathServiceBase
    , public virtual TSupportsList
{
protected:
    void RegisterService(TStringBuf key, TCallback<IYPathServicePtr()> serviceFactory);

private:
    THashMap<TString, TCallback<IYPathServicePtr()>> Services_;

    virtual void ListSelf(
        TReqList* /*request*/,
        TRspList* response,
        const TCtxListPtr& context) override;

    virtual TResolveResult ResolveRecursive(
        const TYPath& path,
        const NRpc::IServiceContextPtr& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
