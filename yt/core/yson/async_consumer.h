#pragma once

#include "public.h"
#include "string.h"
#include "consumer.h"

#include <core/actions/future.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

//! Extends IYsonConsumer by enabling asynchronously constructed
//! segments to be injected into the stream.
struct IAsyncYsonConsumer
    : public IYsonConsumer
{
    using IYsonConsumer::OnRaw;
    virtual void OnRaw(TFuture<TYsonString> asyncStr) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT

