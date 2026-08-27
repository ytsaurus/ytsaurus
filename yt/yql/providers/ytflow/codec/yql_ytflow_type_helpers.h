#pragma once

#include "yql_ytflow_convert_options.h"
#include "yql_ytflow_member_descriptor.h"
#include "yql_ytflow_struct_precomputes.h"

#include <yql/essentials/public/udf/udf_types.h>

#include <yt/yt/client/table_client/public.h>

#include <util/generic/hash.h>


namespace NKikimr::NMiniKQL {

class TType;

} // namespace NKikimr::NMiniKQL

namespace NYql::NUdf {

class IFunctionTypeInfoBuilder;

} // namespace NKikimr::NMiniKQL


namespace NYql::NYtflow::NCodec::NPrivate {

void ValidateTypesCorrespondence(
    const NKikimr::NMiniKQL::TType* type,
    const NYT::NTableClient::TLogicalType* ytType,
    const TConvertOptions& convertOptions);

TStructPrecomputes BuildStructPrecomputes(
    const NKikimr::NMiniKQL::TType* type,
    const NYT::NTableClient::TLogicalType* ytType,
    const TConvertOptions& convertOptions);

THashMap<const NKikimr::NMiniKQL::TType*, const NYql::NUdf::TType*> BuildDictUdfTypes(
    const NKikimr::NMiniKQL::TType* type,
    NYql::NUdf::IFunctionTypeInfoBuilder& functionTypeInfoBuilder);

} // namespace NYql::NYtflow::NCodec::NPrivate
