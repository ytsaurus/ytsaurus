#pragma once

#include "yql_ytflow_convert_options.h"

#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <yt/yt/client/table_client/public.h>

#include <util/generic/ptr.h>


namespace NKikimr::NMiniKQL {

class TType;
class THolderFactory;

} // namespace NKikimr::NMiniKQL


namespace NYql::NYtflow::NCodec {

class IInputCodec {
public:
    virtual NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedRow unversionedRow) = 0;

public:
    virtual ~IInputCodec() = default;
};

THolder<IInputCodec> CreateInputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TTableSchemaPtr ytSchema,
    NYql::NUdf::IValueBuilder& valueBuilder,
    NYql::NUdf::IFunctionTypeInfoBuilder& functionTypeInfoBuilder,
    const TConvertOptions& convertOptions = {});

} // namespace NYql::NYtflow::NCodec
