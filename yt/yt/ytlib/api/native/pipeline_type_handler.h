#pragma once

#include "private.h"

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreatePipelineTypeHandler(TClient* client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative