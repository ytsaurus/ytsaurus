#pragma once

#include "yql_ytflow_configuration.h"
#include "yql_ytflow_gateway.h"

#include <yql/essentials/core/yql_type_annotation.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>


namespace NYql {

struct TYtflowState: public TThrRefBase {
    using TPtr = TIntrusivePtr<TYtflowState>;

    TString SessionId;
    IYtflowGateway::TPtr Gateway;

    TTypeAnnotationContext* Types = nullptr;
    TYtflowConfiguration::TPtr Configuration;
};

} // namespace NYql
