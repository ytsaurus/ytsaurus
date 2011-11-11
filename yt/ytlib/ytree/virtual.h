#pragma once

#include "common.h"
#include "ypath.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TVirtualMapBase
    : public IYPathService
{
public:
    virtual TNavigateResult Navigate(TYPath path);
    virtual TGetResult Get(TYPath path, IYsonConsumer* consumer);
    virtual TSetResult Set(TYPath path, TYsonProducer::TPtr producer);
    virtual TRemoveResult Remove(TYPath path);
    virtual TLockResult Lock(TYPath path);

protected:
    virtual yvector<Stroka> GetKeys() = 0;
    virtual IYPathService::TPtr GetItemService(const Stroka& key) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

