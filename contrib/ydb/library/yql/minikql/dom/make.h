#pragma once

#include <contrib/ydb/library/yql/public/udf/udf_types.h>
#include <contrib/ydb/library/yql/public/udf/udf_value_builder.h>

namespace NYql::NDom {

NUdf::TUnboxedValuePod MakeDom(const NUdf::ITypeInfoHelper* typeHelper, const NUdf::TType* shape, const NUdf::TUnboxedValuePod value, const NUdf::IValueBuilder* valueBuilder);

}
