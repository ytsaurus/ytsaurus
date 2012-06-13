#pragma once

#include "public.h"
#include "yson_string.h"

#include <ytlib/actions/callback.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! A callback capable of generating YSON by calling appropriate
//! methods for its IYsonConsumer argument.
typedef TCallback<void(IYsonConsumer*)> TYsonCallback;

////////////////////////////////////////////////////////////////////////////////

class TYsonProducer
{
public:
    TYsonProducer(TYsonCallback callback, EYsonType ysonType = EYsonType::Node);
    void Run(IYsonConsumer* consumer) const;
    DEFINE_BYVAL_RO_PROPERTY(EYsonType, Type);

private:
    TYsonCallback Callback;
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonProducer& value, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
