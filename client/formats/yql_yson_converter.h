#include "public.h"

#include <yt/client/table_client/public.h>

#include <yt/core/json/public.h>

#include <yt/core/yson/consumer.h>

#include <util/generic/buffer.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TYqlJsonConsumer
    : public NYson::IYsonConsumer
{
public:
    explicit TYqlJsonConsumer(NJson::IJsonConsumer* underlying);

    void OnStringScalar(TStringBuf value) final;

    void OnInt64Scalar(i64 value) final;
    void OnUint64Scalar(ui64 value) final;
    void OnDoubleScalar(double value) final;
    void OnBooleanScalar(bool value) final;
    void OnEntity() final;

    void OnBeginList() final;
    void OnListItem() final;
    void OnEndList() final;

    void OnBeginMap() final;
    void OnKeyedItem(TStringBuf key) final;
    void OnEndMap() final;

    void OnBeginAttributes() final;
    void OnEndAttributes() final;

    void OnRaw(TStringBuf yson, NYson::EYsonType type) final;

    void TransferYson(const std::function<void(NYson::IYsonConsumer*)>& callback);

private:
    NJson::IJsonConsumer* const Underlying_;
    TBuffer Buffer_;
};

using TYsonToYqlConverter = std::function<void(NYson::TYsonPullParserCursor*, TYqlJsonConsumer*)>;
using TUnversionedValueToYqlConverter = std::function<void(NTableClient::TUnversionedValue, TYqlJsonConsumer*)>;

// Created converters throw exceptions on schema incompliance.
TYsonToYqlConverter CreateYsonToYqlConverter(const NTableClient::TLogicalTypePtr& logicalType);
TUnversionedValueToYqlConverter CreateUnversionedValueToYqlConverter(const NTableClient::TLogicalTypePtr& logicalType);


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats