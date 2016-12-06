#pragma once

#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TServiceCombiner
    : public TSupportsAttributes
{
public:
    TServiceCombiner(std::vector<IYPathServicePtr> services, TNullable<TDuration> keysUpdatePeriod = Null);

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override;

    virtual TResolveResult ResolveRecursive(const TYPath& path, NRpc::IServiceContextPtr context) override;
    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context) override;
    virtual void ListSelf(TReqList* request, TRspList* response, TCtxListPtr context) override;

private:
    std::vector<IYPathServicePtr> Services_;
    typedef TErrorOr<yhash_map<Stroka, IYPathServicePtr>> TKeyMappingOrError;
    TKeyMappingOrError KeyMapping_;

    NConcurrency::TPeriodicExecutorPtr UpdateKeysExecutor_;

    TSpinLock KeyMappingSpinLock_;

    TBuiltinAttributeKeysCache BuiltinAttributeKeysCache_;

    void UpdateKeys();

    void ValidateKeyMapping();
    void SetKeyMapping(TKeyMappingOrError keyMapping);

    void ListSelfUsingKeyMapping(TReqList* request, TRspList* response, TCtxListPtr context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

