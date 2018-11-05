#pragma once

#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TServiceCombiner
    : public TYPathServiceBase
{
public:
    explicit TServiceCombiner(
        std::vector<IYPathServicePtr> services,
        TNullable<TDuration> keysUpdatePeriod = Null);

    void SetUpdatePeriod(TDuration period);

    virtual TResolveResult Resolve(const TYPath& path, const NRpc::IServiceContextPtr& context) override;
    virtual void Invoke(const NRpc::IServiceContextPtr& context) override;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

