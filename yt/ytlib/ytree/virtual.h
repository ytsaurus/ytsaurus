#pragma once

#include "common.h"
#include "ypath_service.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TVirtualMapBase
    : public IYPathService
{
public:
    virtual TNavigateResult Navigate(TYPath path, bool mustExist);

    virtual void Invoke(NRpc::IServiceContext* context);

protected:
    virtual yvector<Stroka> GetKeys() = 0;
    virtual IYPathService::TPtr GetItemService(const Stroka& key) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

