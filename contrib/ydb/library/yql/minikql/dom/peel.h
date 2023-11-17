#pragma once

#include <contrib/ydb/library/yql/public/udf/udf_types.h>
#include <contrib/ydb/library/yql/public/udf/udf_value_builder.h>

namespace NYql::NDom {

template<bool Strict, bool AutoConvert>
NUdf::TUnboxedValuePod PeelDom(const NUdf::ITypeInfoHelper* typeHelper, const NUdf::TType* shape, const NUdf::TUnboxedValuePod value, const NUdf::IValueBuilder* valueBuilder, const NUdf::TSourcePosition& pos);

}
