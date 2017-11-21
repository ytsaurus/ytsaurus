#pragma once

#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TServiceCombiner
    : public TSupportsAttributes
{
public:
    explicit TServiceCombiner(
        std::vector<IYPathServicePtr> services,
        TNullable<TDuration> keysUpdatePeriod = Null);

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    virtual TResolveResult ResolveRecursive(const TYPath& path, const NRpc::IServiceContextPtr& context) override;
    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;
    virtual void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override;

private:
    const std::vector<IYPathServicePtr> Services_;
    typedef TErrorOr<THashMap<TString, IYPathServicePtr>> TKeyMappingOrError;
    TKeyMappingOrError KeyMapping_;

    NConcurrency::TPeriodicExecutorPtr UpdateKeysExecutor_;

    TSpinLock KeyMappingSpinLock_;

    TBuiltinAttributeKeysCache BuiltinAttributeKeysCache_;

    void UpdateKeys();

    void ValidateKeyMapping();
    void SetKeyMapping(TKeyMappingOrError keyMapping);

    void ListSelfUsingKeyMapping(
        TReqList* request,
        TRspList* response,
        const TCtxListPtr& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

