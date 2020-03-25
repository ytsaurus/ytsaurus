#include "public.h"

#include <yt/client/table_client/public.h>

#include <yt/core/json/public.h>

#include <yt/core/yson/consumer.h>

#include <util/generic/buffer.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TYqlConverterConfig
    : public TIntrinsicRefCounted
{
    std::optional<i64> StringWeightLimit;
    std::optional<i64> FieldWeightLimit;
};
DECLARE_REFCOUNTED_STRUCT(TYqlConverterConfig);
DEFINE_REFCOUNTED_TYPE(TYqlConverterConfig);

////////////////////////////////////////////////////////////////////////////////

using TYsonToYqlConverter = std::function<void(NYson::TYsonPullParserCursor*)>;
using TUnversionedValueToYqlConverter = std::function<void(NTableClient::TUnversionedValue)>;

// Created converters throw exceptions on schema incompliance.
TYsonToYqlConverter CreateYsonToYqlConverter(
    const NTableClient::TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config,
    NJson::IJsonWriter* writer);
TUnversionedValueToYqlConverter CreateUnversionedValueToYqlConverter(
    const NTableClient::TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config,
    NJson::IJsonWriter* writer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
