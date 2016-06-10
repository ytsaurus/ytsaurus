#include "producer.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

TYsonProducer::TYsonProducer()
{ }

TYsonProducer::TYsonProducer(TYsonCallback callback, EYsonType type)
    : Type_(type)
    , Callback_(std::move(callback))
{
    Y_ASSERT(Callback_);
}

void TYsonProducer::Run(IYsonConsumer* consumer) const
{
    Callback_.Run(consumer);
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonProducer& value, IYsonConsumer* consumer)
{
    value.Run(consumer);
}

void Serialize(const TYsonCallback& value, IYsonConsumer* consumer)
{
    Serialize(TYsonProducer(value), consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
