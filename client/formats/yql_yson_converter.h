#include "public.h"

#include <yt/client/table_client/public.h>

#include <yt/core/json/public.h>

#include <yt/core/yson/consumer.h>

#include <util/generic/buffer.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TYqlJsonConsumer
{
public:
    explicit TYqlJsonConsumer(NJson::IJsonConsumer* underlying);

    i64 OnInt64Scalar(i64 value);
    i64 OnUint64Scalar(ui64 value);
    i64 OnDoubleScalar(double value);
    i64 OnBooleanScalar(bool value);
    i64 OnEntity();

    i64 OnBeginList();
    i64 OnListItem();
    i64 OnEndList();

    i64 OnBeginMap();
    i64 OnKeyedItem(TStringBuf key);
    i64 OnEndMap();

    i64 OnStringScalarWeightLimited(TStringBuf value, i64 limit);
    i64 TransferYsonWeightLimited(const std::function<void(NYson::IYsonConsumer*)>& callback, i64 limit);

private:
    NJson::IJsonConsumer* const Underlying_;
    TBuffer Buffer_;
    
    static constexpr i64 BeginListWeight = 1;
    static constexpr i64 ListItemWeight = 1;
    static constexpr i64 EndListWeight = 1;
    static constexpr i64 BeginMapWeight = 1;
    static constexpr i64 EndMapWeight = 1;
    static constexpr i64 EntityWeight = sizeof("null") - 1;
    static constexpr i64 BooleanScalarWeight = sizeof("false") - 1;

public:
    static constexpr auto KeyValue = AsStringBuf("val");
    static constexpr auto KeyIncomplete = AsStringBuf("inc");
    static constexpr auto KeyBase64 = AsStringBuf("b64");

private:
    i64 OnStringScalarImpl(TStringBuf value, bool incomplete = false, bool base64 = false);

    inline i64 GetStringWeight(TStringBuf s)
    {
        return 2 + s.size();
    }

    inline i64 GetKeyedItemWeight(TStringBuf s)
    {
        return 1 + GetStringWeight(s);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TYqlConverterConfig
    : public TIntrinsicRefCounted
{
    i64 StringWeightLimit = std::numeric_limits<i64>::max();
    i64 FieldWeightLimit = std::numeric_limits<i64>::max();
};
DECLARE_REFCOUNTED_STRUCT(TYqlConverterConfig);
DEFINE_REFCOUNTED_TYPE(TYqlConverterConfig);

////////////////////////////////////////////////////////////////////////////////

using TYsonToYqlConverter = std::function<void(NYson::TYsonPullParserCursor*, TYqlJsonConsumer*)>;
using TUnversionedValueToYqlConverter = std::function<void(NTableClient::TUnversionedValue, TYqlJsonConsumer*)>;

// Created converters throw exceptions on schema incompliance.
TYsonToYqlConverter CreateYsonToYqlConverter(
    const NTableClient::TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config);
TUnversionedValueToYqlConverter CreateUnversionedValueToYqlConverter(
    const NTableClient::TLogicalTypePtr& logicalType,
    TYqlConverterConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
