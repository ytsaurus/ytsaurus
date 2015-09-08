#pragma once

#include "public.h"
#include "value_consumer.h"

#include <core/misc/error.h>

#include <core/yson/consumer.h>
#include <core/yson/writer.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETableConsumerControlState,
    (None)
    (ExpectName)
    (ExpectValue)
    (ExpectEndAttributes)
    (ExpectEntity)
);

class TTableConsumer
    : public NYson::TYsonConsumerBase
{
public:
    explicit TTableConsumer(IValueConsumerPtr consumer);
    explicit TTableConsumer(
        const std::vector<IValueConsumerPtr>& consumers,
        int tableIndex = 0);

protected:
    using EControlState = ETableConsumerControlState;

    TError AttachLocationAttributes(TError error);

    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& name) override;
    virtual void OnEndMap() override;

    virtual void OnBeginAttributes() override;

    void ThrowMapExpected();
    void ThrowControlAttributesNotSupported();
    void ThrowInvalidControlAttribute(const Stroka& whatsWrong);

    virtual void OnEndList() override;
    virtual void OnEndAttributes() override;

    void OnControlInt64Scalar(i64 value);
    void OnControlStringScalar(const TStringBuf& value);


    void FlushCurrentValueIfCompleted();

    std::vector<IValueConsumerPtr> ValueConsumers_;
    IValueConsumer* CurrentValueConsumer_;

    EControlState ControlState_ = EControlState::None;
    EControlAttribute ControlAttribute_;

    TBlobOutput ValueBuffer_;
    NYson::TYsonWriter ValueWriter_;

    int Depth_ = 0;
    int ColumnIndex_ = 0;

    i64 RowIndex_ = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
