#pragma once

#include "private.h"

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateQueueConsumerTypeHandler(TClient* client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative