#ifndef YPATH_SERVICE_INL_H_
#error "Direct inclusion of this file is not allowed, include ypath_service.h"
#endif

#include "yt/core/yson/producer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T, class R>
IYPathServicePtr IYPathService::FromMethod(R (T::*method) () const, TWeakPtr<T> weak)
{
    auto producer = NYson::TYsonProducer(BIND([=] (NYson::IYsonConsumer* consumer) {
        auto strong = weak.Lock();
        if (strong) {
            Serialize((strong.Get()->*method)(), consumer);
        }
    }));

    return FromProducer(producer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

