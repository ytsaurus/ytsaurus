#pragma once

#include "public.h"

#include <yt/ytlib/table_client/row_base.h>
#include <yt/core/misc/variant.h>

namespace NYT {
namespace NQueryClient {

using NTableClient::EValueType;

////////////////////////////////////////////////////////////////////////////////

typedef int TTypeArgument;
typedef std::vector<EValueType> TUnionType;
typedef TVariant<EValueType, TTypeArgument, TUnionType> TType;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECallingConvention,
    (Simple)
    (UnversionedValue)
);

DEFINE_ENUM(ETypeCategory,
    ((TypeArgument) (TType::TagOf<TTypeArgument>()))
    ((UnionType)    (TType::TagOf<TUnionType>()))
    ((ConcreteType) (TType::TagOf<EValueType>()))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT