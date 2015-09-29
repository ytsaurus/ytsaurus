#include "stdafx.h"

#include "table_consumer.h"

#include "name_table.h"

#include <core/concurrency/scheduler.h>

#include <cmath>

namespace NYT {
namespace NTableClient {

using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TTableConsumer::TTableConsumer(const std::vector<IValueConsumerPtr>& valueConsumers, int tableIndex)
    : ValueConsumers_(valueConsumers)
    , ValueWriter_(&ValueBuffer_)
{
    YCHECK(!ValueConsumers_.empty());
    YCHECK(ValueConsumers_.size() > tableIndex);
    YCHECK(tableIndex >= 0);
    CurrentValueConsumer_ = ValueConsumers_[tableIndex].Get();
}

TTableConsumer::TTableConsumer(IValueConsumerPtr valueConsumer)
    : TTableConsumer(std::vector<IValueConsumerPtr>(1, valueConsumer))
{ }

TError TTableConsumer::AttachLocationAttributes(TError error)
{
    return error << TErrorAttribute("row_index", RowIndex_);
}

void TTableConsumer::OnControlInt64Scalar(i64 value)
{
    switch (ControlAttribute_) {
        case EControlAttribute::TableIndex:
            if (value >= ValueConsumers_.size()) {
                THROW_ERROR AttachLocationAttributes(TError("Invalid table index %v", value));
            }
            CurrentValueConsumer_ = ValueConsumers_[value].Get();
            break;

        default:
            ThrowControlAttributesNotSupported();
    }
}

void TTableConsumer::OnControlStringScalar(const TStringBuf& /*value*/)
{
    ThrowControlAttributesNotSupported();
}

void TTableConsumer::OnStringScalar(const TStringBuf& value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        OnControlStringScalar(value);
        ControlState_ = EControlState::ExpectEndAttributes;
        return;
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else if (Depth_ == 1) {
        CurrentValueConsumer_->OnValue(MakeUnversionedStringValue(value, ColumnIndex_));
    } else {
        ValueWriter_.OnStringScalar(value);
    }
}

void TTableConsumer::OnInt64Scalar(i64 value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        OnControlInt64Scalar(value);
        ControlState_ = EControlState::ExpectEndAttributes;
        return;
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else if (Depth_ == 1) {
        CurrentValueConsumer_->OnValue(MakeUnversionedInt64Value(value, ColumnIndex_));
    } else {
        ValueWriter_.OnInt64Scalar(value);
    }
}

void TTableConsumer::OnUint64Scalar(ui64 value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        ThrowInvalidControlAttribute("be an unsigned integer");
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else if (Depth_ == 1) {
        CurrentValueConsumer_->OnValue(MakeUnversionedUint64Value(value, ColumnIndex_));
    } else {
        ValueWriter_.OnUint64Scalar(value);
    }
}

void TTableConsumer::OnDoubleScalar(double value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a double value");
        return;
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else if (Depth_ == 1) {
        if (std::isnan(value)) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::InvalidDoubleValue, 
               "Failed to parse double value: %Qv is not a valid double",
                value);
        }

        CurrentValueConsumer_->OnValue(MakeUnversionedDoubleValue(value, ColumnIndex_));
    } else {
        ValueWriter_.OnDoubleScalar(value);
    }
}

void TTableConsumer::OnBooleanScalar(bool value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a boolean value");
        return;
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else if (Depth_ == 1) {
        CurrentValueConsumer_->OnValue(MakeUnversionedBooleanValue(value, ColumnIndex_));
    } else {
        ValueWriter_.OnBooleanScalar(value);
    }
}

void TTableConsumer::OnEntity()
{
    switch (ControlState_) {
        case EControlState::None:
            break;

        case EControlState::ExpectEntity:
            YASSERT(Depth_ == 0);
            // Successfully processed control statement.
            ControlState_ = EControlState::None;
            return;

        case EControlState::ExpectValue:
            ThrowInvalidControlAttribute("be an entity");
            break;

        default:
            YUNREACHABLE();
    }

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else if (Depth_ == 1) {
        CurrentValueConsumer_->OnValue(MakeUnversionedSentinelValue(EValueType::Null, ColumnIndex_));
    } else {
        ValueWriter_.OnEntity();
    }
}

void TTableConsumer::OnBeginList()
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a list");
        return;
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter_.OnBeginList();
    }
    ++Depth_;
}

void TTableConsumer::OnBeginAttributes()
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("have attributes");
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ControlState_ = EControlState::ExpectName;
    } else if (Depth_ == 1) {
        THROW_ERROR_EXCEPTION("Table values cannot have top-level attributes");
    } else {
        ValueWriter_.OnBeginAttributes();
    }

    ++Depth_;
}

void TTableConsumer::ThrowControlAttributesNotSupported()
{
    THROW_ERROR AttachLocationAttributes(TError("Control attributes are not supported"));
}

void TTableConsumer::ThrowMapExpected()
{
    THROW_ERROR AttachLocationAttributes(TError("Invalid row format, map expected"));
}

void TTableConsumer::ThrowInvalidControlAttribute(const Stroka& whatsWrong)
{
    THROW_ERROR AttachLocationAttributes(TError("Control attribute %Qlv cannot %v",
        ControlAttribute_,
        whatsWrong));
}

void TTableConsumer::OnListItem()
{
    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        // Row separator, do nothing.
    } else {
        ValueWriter_.OnListItem();
    }
}

void TTableConsumer::OnBeginMap()
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a map");
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        CurrentValueConsumer_->OnBeginRow();
    } else {
        ValueWriter_.OnBeginMap();
    }
    ++Depth_;
}

void TTableConsumer::OnKeyedItem(const TStringBuf& name)
{
    switch (ControlState_) {
        case EControlState::None:
            break;

        case EControlState::ExpectName:
            YASSERT(Depth_ == 1);
            try {
                ControlAttribute_ = ParseEnum<EControlAttribute>(ToString(name));
            } catch (const std::exception&) {
                // Ignore ex, our custom message is more meaningful.
                THROW_ERROR AttachLocationAttributes(TError("Failed to parse control attribute name %Qv", name));
            }
            ControlState_ = EControlState::ExpectValue;
            return;

        case EControlState::ExpectEndAttributes:
            YASSERT(Depth_ == 1);
            THROW_ERROR AttachLocationAttributes(TError("Too many control attributes per record: at most one attribute is allowed"));
            break;

        default:
            YUNREACHABLE();
    }

    YASSERT(Depth_ > 0);
    if (Depth_ == 1) {
        if (CurrentValueConsumer_->GetAllowUnknownColumns()) {
            ColumnIndex_ = CurrentValueConsumer_->GetNameTable()->GetIdOrRegisterName(name);
        } else {
            // TODO(savrus): Imporve diagnostics when encountered a computed column.
            auto maybeIndex = CurrentValueConsumer_->GetNameTable()->FindId(name);
            if (!maybeIndex) {
                THROW_ERROR AttachLocationAttributes(TError("No such column %Qv in schema",
                    name));
            }
            ColumnIndex_ = *maybeIndex;
        }
    } else {
        ValueWriter_.OnKeyedItem(name);
    }
}

void TTableConsumer::OnEndMap()
{
    YASSERT(Depth_ > 0);
    // No control attribute allows map or composite values.
    YASSERT(ControlState_ == EControlState::None);

    --Depth_;
    if (Depth_ > 0) {
        ValueWriter_.OnEndMap();
        FlushCurrentValueIfCompleted();
    } else {
        CurrentValueConsumer_->OnEndRow();
        ++RowIndex_;
    }
}

void TTableConsumer::OnEndList()
{
    // No control attribute allow list or composite values.
    YASSERT(ControlState_ == EControlState::None);

    --Depth_;
    YASSERT(Depth_ > 0);

    ValueWriter_.OnEndList();
    FlushCurrentValueIfCompleted();
}

void TTableConsumer::OnEndAttributes()
{
    --Depth_;

    switch (ControlState_) {
        case EControlState::ExpectName:
            THROW_ERROR AttachLocationAttributes(TError("Too few control attributes per record: at least one attribute is required"));
            break;

        case EControlState::ExpectEndAttributes:
            YASSERT(Depth_ == 0);
            ControlState_ = EControlState::ExpectEntity;
            break;

        case EControlState::None:
            YASSERT(Depth_ > 0);
            ValueWriter_.OnEndAttributes();
            break;

        default:
            YUNREACHABLE();
    }
}

void TTableConsumer::FlushCurrentValueIfCompleted()
{
    if (Depth_ == 1) {
        CurrentValueConsumer_->OnValue(MakeUnversionedAnyValue(
            TStringBuf(
                ValueBuffer_.Begin(), 
                ValueBuffer_.Begin() + ValueBuffer_.Size()),
            ColumnIndex_));
        ValueBuffer_.Clear();
    }   
}

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
