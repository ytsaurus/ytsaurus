#include "yql_ytflow_convert_options.h"

#include <yql/essentials/utils/yql_panic.h>


namespace NYql::NYtflow::NCodec {

TConvertOptions& TConvertOptions::WithConvertDirection(EConvertDirection value) {
    YQL_ENSURE(value != EConvertDirection::Invalid);
    ConvertDirection = value;
    return *this;
}

TConvertOptions& TConvertOptions::WithAllowExtraYtFields(bool value) {
    YQL_ENSURE(!(value && AllowExtraYqlFields));
    AllowExtraYtFields = value;
    return *this;
}

TConvertOptions& TConvertOptions::WithAllowExtraYqlFields(bool value) {
    YQL_ENSURE(!(value && AllowExtraYtFields));
    AllowExtraYqlFields = value;
    return *this;
}

EConvertDirection TConvertOptions::GetConvertDirection() const {
    return ConvertDirection;
}

bool TConvertOptions::GetAllowExtraYtFields() const {
    return AllowExtraYtFields;
}

bool TConvertOptions::GetAllowExtraYqlFields() const {
    return AllowExtraYqlFields;
}

} // namespace NYql::NYtflow::NCodec
