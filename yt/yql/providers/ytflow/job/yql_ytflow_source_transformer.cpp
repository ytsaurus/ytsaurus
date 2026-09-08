#include "yql_ytflow_source_transformer.h"

#include "yql_ytflow_default_source_transformer.h"
#include "yql_ytflow_logbroker_source_transformer.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NYtflow {

THolder<ISourceTransformer> CreateSourceTransformer(ESourceType sourceType) {
    switch (sourceType) {
        case ESourceType::YT:
            return CreateDefaultSourceTransformer();
        case ESourceType::Logbroker:
            return CreateLogbrokerSourceTransformer();
        default:
            YQL_ENSURE(false, "Unexpected source type");
    }
}

} // namespace NYql::NYtflow
