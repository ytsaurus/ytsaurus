#include "table_consumer.h"
#include "name_table.h"

#include <yt/core/concurrency/scheduler.h>

#include <cmath>

namespace NYT {
namespace NTableClient {

using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TTableConsumer::TTableConsumer(
    std::vector<IValueConsumer*> valueConsumers,
    int tableIndex)
    : ValueConsumers_(std::move(valueConsumers))
    , ValueWriter_(&ValueBuffer_)
{
    for (auto* consumer : ValueConsumers_) {
        NameTableWriters_.emplace_back(std::make_unique<TNameTableWriter>(consumer->GetNameTable()));
    }
    SwitchToTable(tableIndex);
}

TTableConsumer::TTableConsumer(IValueConsumer* valueConsumer)
    : TTableConsumer(std::vector<IValueConsumer*>(1, valueConsumer))
{ }

TError TTableConsumer::AttachLocationAttributes(TError error)
{
    return error << TErrorAttribute("row_index", RowIndex_);
}

void TTableConsumer::OnControlInt64Scalar(i64 value)
{
    switch (ControlAttribute_) {
        case EControlAttribute::TableIndex:
            if (value < 0 || value >= ValueConsumers_.size()) {
                THROW_ERROR AttachLocationAttributes(TError(
                    "Invalid table index %v: expected integer in range [0,%v]",
                    value,
                    ValueConsumers_.size() - 1));
            }
            SwitchToTable(value);
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
        Y_ASSERT(Depth_ == 1);
        OnControlStringScalar(value);
        ControlState_ = EControlState::ExpectEndAttributes;
        return;
    }

    Y_ASSERT(ControlState_ == EControlState::None);

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
        Y_ASSERT(Depth_ == 1);
        OnControlInt64Scalar(value);
        ControlState_ = EControlState::ExpectEndAttributes;
        return;
    }

    Y_ASSERT(ControlState_ == EControlState::None);

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

    Y_ASSERT(ControlState_ == EControlState::None);

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
        Y_ASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a double value");
        return;
    }

    Y_ASSERT(ControlState_ == EControlState::None);

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
        Y_ASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a boolean value");
        return;
    }

    Y_ASSERT(ControlState_ == EControlState::None);

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
            Y_ASSERT(Depth_ == 0);
            // Successfully processed control statement.
            ControlState_ = EControlState::None;
            return;

        case EControlState::ExpectValue:
            ThrowInvalidControlAttribute("be an entity");
            break;

        default:
            Y_UNREACHABLE();
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
        Y_ASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a list");
        return;
    }

    Y_ASSERT(ControlState_ == EControlState::None);

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
        Y_ASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("have attributes");
    }

    Y_ASSERT(ControlState_ == EControlState::None);

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

void TTableConsumer::ThrowInvalidControlAttribute(const TString& whatsWrong)
{
    THROW_ERROR AttachLocationAttributes(TError("Control attribute %Qlv cannot %v",
        ControlAttribute_,
        whatsWrong));
}

void TTableConsumer::OnListItem()
{
    Y_ASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        // Row separator, do nothing.
    } else {
        ValueWriter_.OnListItem();
    }
}

void TTableConsumer::OnBeginMap()
{
    if (ControlState_ == EControlState::ExpectValue) {
        Y_ASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a map");
    }

    Y_ASSERT(ControlState_ == EControlState::None);

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
            Y_ASSERT(Depth_ == 1);
            try {
                ControlAttribute_ = ParseEnum<EControlAttribute>(ToString(name));
            } catch (const std::exception&) {
                // Ignore ex, our custom message is more meaningful.
                THROW_ERROR AttachLocationAttributes(TError("Failed to parse control attribute name %Qv", name));
            }
            ControlState_ = EControlState::ExpectValue;
            return;

        case EControlState::ExpectEndAttributes:
            Y_ASSERT(Depth_ == 1);
            THROW_ERROR AttachLocationAttributes(TError("Too many control attributes per record: at most one attribute is allowed"));

        default:
            Y_UNREACHABLE();
    }

    Y_ASSERT(Depth_ > 0);
    if (Depth_ == 1) {
        if (CurrentValueConsumer_->GetAllowUnknownColumns()) {
            try {
                ColumnIndex_ = CurrentNameTableWriter_->GetIdOrRegisterName(name);
            } catch (const std::exception& ex) {
                THROW_ERROR AttachLocationAttributes(TError("Failed to add column to name table for table writer") 
                    << ex);
            }
        } else {
            try {
                ColumnIndex_ = CurrentNameTableWriter_->GetIdOrThrow(name);
            } catch (const std::exception& ex) {
                THROW_ERROR AttachLocationAttributes(ex);
            }
        }
    } else {
        ValueWriter_.OnKeyedItem(name);
    }
}

void TTableConsumer::OnEndMap()
{
    Y_ASSERT(Depth_ > 0);
    // No control attribute allows map or composite values.
    Y_ASSERT(ControlState_ == EControlState::None);

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
    Y_ASSERT(ControlState_ == EControlState::None);

    --Depth_;
    Y_ASSERT(Depth_ > 0);

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
            Y_ASSERT(Depth_ == 0);
            ControlState_ = EControlState::ExpectEntity;
            break;

        case EControlState::None:
            Y_ASSERT(Depth_ > 0);
            ValueWriter_.OnEndAttributes();
            break;

        default:
            Y_UNREACHABLE();
    }
}

void TTableConsumer::FlushCurrentValueIfCompleted()
{
    if (Depth_ == 1) {
        ValueWriter_.Flush();
        CurrentValueConsumer_->OnValue(MakeUnversionedAnyValue(
            TStringBuf(
                ValueBuffer_.Begin(),
                ValueBuffer_.Begin() + ValueBuffer_.Size()),
            ColumnIndex_));
        ValueBuffer_.Clear();
    }
}

void TTableConsumer::SwitchToTable(int tableIndex)
{
    YCHECK(tableIndex >= 0 && tableIndex < ValueConsumers_.size());
    CurrentValueConsumer_ = ValueConsumers_[tableIndex];
    CurrentNameTableWriter_ = NameTableWriters_[tableIndex].get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
