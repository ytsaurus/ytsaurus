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

    virtual void OnStringScalar(TStringBuf value) final;

    virtual void OnInt64Scalar(i64 value) final;
    virtual void OnUint64Scalar(ui64 value) final;
    virtual void OnDoubleScalar(double value) final;
    virtual void OnBooleanScalar(bool value) final;
    virtual void OnEntity() final;

    virtual void OnBeginList() final;
    virtual void OnListItem() final;
    virtual void OnEndList() final;

    virtual void OnBeginMap() final;
    virtual void OnKeyedItem(TStringBuf key) final;
    virtual void OnEndMap() final;

    virtual void OnBeginAttributes() final;
    virtual void OnEndAttributes() final;

    virtual void OnRaw(TStringBuf yson, NYson::EYsonType type) final;

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
