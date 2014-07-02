#pragma once

#include "public.h"
#include "yson_string.h"

#include <core/actions/callback.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! A callback capable of generating YSON by calling appropriate
//! methods for its IYsonConsumer argument.
typedef TCallback<void(NYson::IYsonConsumer*)> TYsonCallback;

////////////////////////////////////////////////////////////////////////////////

//! A TYsonCallback annotated with type.
class TYsonProducer
{
public:
    TYsonProducer(
        TYsonCallback callback,
        NYson::EYsonType type = NYson::EYsonType::Node);

    void Run(NYson::IYsonConsumer* consumer) const;

    DEFINE_BYVAL_RO_PROPERTY(NYson::EYsonType, Type);

private:
    TYsonCallback Callback;

};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TYsonProducer& value, NYson::IYsonConsumer* consumer);
void Serialize(const TYsonCallback& value, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
