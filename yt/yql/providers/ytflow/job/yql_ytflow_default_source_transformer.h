#pragma once

#include "yql_ytflow_source_transformer.h"

namespace NYql::NYtflow {

THolder<ISourceTransformer> CreateDefaultSourceTransformer();

} // namespace NYql::NYtflow
