#pragma once

#include "yql_ytflow_source_transformer.h"

namespace NYql::NYtflow {

THolder<ISourceTransformer> CreateLogbrokerSourceTransformer();

} // namespace NYql::NYtflow
