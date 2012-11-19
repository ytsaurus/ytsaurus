#include "stdafx.h"
#include "yson_producer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TYsonProducer::TYsonProducer(TYsonCallback callback, EYsonType type)
    : Type_(type)
    , Callback(callback)
{
    YASSERT(!Callback.IsNull());
}

void TYsonProducer::Run(IYsonConsumer* consumer) const
{
    Callback.Run(consumer);
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonProducer& value, IYsonConsumer* consumer)
{
    value.Run(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
