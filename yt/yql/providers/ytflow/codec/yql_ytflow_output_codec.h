#pragma once

#include "yql_ytflow_convert_options.h"

#include <yql/essentials/public/udf/udf_value.h>

#include <yt/yt/client/table_client/public.h>

#include <util/generic/ptr.h>


namespace NKikimr::NMiniKQL {

class TType;

} // namespace NKikimr::NMiniKQL


namespace NYql::NYtflow::NCodec {

class IOutputCodec {
public:
    virtual NYT::NTableClient::TUnversionedRow Convert(
        NYql::NUdf::TUnboxedValue unboxedValue) = 0;

public:
    virtual ~IOutputCodec() = default;
};

THolder<IOutputCodec> CreateOutputCodec(
    const NKikimr::NMiniKQL::TType* type,
    NYT::NTableClient::TTableSchemaPtr ytSchema,
    NYT::NTableClient::TRowBufferPtr rowBuffer,
    const TConvertOptions& convertOptions = {});

} // namespace NYql::NYtflow::NCodec
