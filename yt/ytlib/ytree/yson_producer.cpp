#include "stdafx.h"
#include "yson_producer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TYsonProducer::TYsonProducer(TYsonCallback callback, EYsonType type)
    : Callback(callback)
    , Type_(type)
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
