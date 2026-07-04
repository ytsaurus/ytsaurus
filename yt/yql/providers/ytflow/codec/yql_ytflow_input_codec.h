#pragma once

#include "yql_ytflow_convert_options.h"

#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <util/generic/ptr.h>


namespace NKikimr::NMiniKQL {

class TType;
class THolderFactory;

} // namespace NKikimr::NMiniKQL


namespace NYql::NYtflow::NCodec {

class IRowInputCodec {
public:
    virtual NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedRow unversionedRow) = 0;

public:
    virtual ~IRowInputCodec() = default;
};

class IValueInputCodec {
public:
    virtual NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedValue unversionedValue) = 0;

    virtual NYql::NUdf::TUnboxedValue Convert(
        NYT::NTableClient::TUnversionedValueRange unversionedValues) = 0;

public:
    virtual ~IValueInputCodec() = default;
};

THolder<IRowInputCodec> CreateRowInputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TTableSchemaPtr ytSchema,
    NYql::NUdf::IValueBuilder& valueBuilder,
    NYql::NUdf::IFunctionTypeInfoBuilder& functionTypeInfoBuilder,
    const TConvertOptions& convertOptions = {});

THolder<IValueInputCodec> CreateValueInputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TLogicalTypePtr ytType,
    NYql::NUdf::IValueBuilder& valueBuilder,
    NYql::NUdf::IFunctionTypeInfoBuilder& functionTypeInfoBuilder,
    const TConvertOptions& convertOptions = {});

} // namespace NYql::NYtflow::NCodec
