#include "stdafx.h"
#include "sync_writer.h"
#include "table_consumer.h"
#include "config.h"

#include <core/misc/string.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schema.h>

namespace NYT {
namespace NTableClient {

using namespace NYson;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

void TTableConsumer::OnStringScalar(const TStringBuf& value)
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a string value");
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnStringScalar(value);
    }
}

void TTableConsumer::OnInt64Scalar(i64 value)
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);

        switch (ControlAttribute) {
            case EControlAttribute::TableIndex: {
                if (value < 0 || value >= Writers.size()) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid table index: expected in range [0, %d], actual %" PRId64,
                        static_cast<int>(Writers.size()) - 1,
                        value)
                        << TErrorAttribute("row_index", Writer->GetRowCount());
                }
                CurrentTableIndex = value;
                Writer = Writers[CurrentTableIndex];
                ControlState = EControlState::ExpectEndControlAttributes;
                break;
            }

            default:
                ThrowInvalidControlAttribute("be an integer value");
        }

        return;
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnInt64Scalar(value);
    }
}

void TTableConsumer::OnDoubleScalar(double value)
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a double value");
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnDoubleScalar(value);
    }
}

void TTableConsumer::OnBooleanScalar(bool value)
{
    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnBooleanScalar(value);
    }
}

void TTableConsumer::OnEntity()
{
    switch (ControlState) {
        case EControlState::None:
            break;

        case EControlState::ExpectEntity:
	        YCHECK(Depth == 0);
            // Successfully processed control statement.
            ControlState = EControlState::None;
            return;

	    case EControlState::ExpectControlAttributeValue:
            ThrowInvalidControlAttribute("be an entity");
            break;

        default:
            YUNREACHABLE();
    }


    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnEntity();
    }
}

void TTableConsumer::OnBeginList()
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a list");
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ++Depth;
        ValueWriter.OnBeginList();
    }
}

void TTableConsumer::OnBeginAttributes()
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("have attributes");
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        ControlState = EControlState::ExpectControlAttributeName;
    } else {
        ValueWriter.OnBeginAttributes();
    }

    ++Depth;
}

void TTableConsumer::OnListItem()
{
    YCHECK(ControlState == EControlState::None);

    if (Depth == 0) {
        // Row separator, do nothing.
    } else {
        ValueWriter.OnListItem();
    }
}

void TTableConsumer::OnBeginMap()
{
    if (ControlState == EControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a map");
    }

    if (ControlState == EControlState::ExpectEntity) {
        ThrowEntityExpected();
    }

    YCHECK(ControlState == EControlState::None);

    if (Depth > 0) {
        ValueWriter.OnBeginMap();
    }

    ++Depth;
}

void TTableConsumer::OnKeyedItem(const TStringBuf& name)
{
    switch (ControlState) {
   	    case EControlState::None:
            break;

        case EControlState::ExpectControlAttributeName:
            YCHECK(Depth == 1);
            try {
                ControlAttribute = ParseEnum<EControlAttribute>(ToString(name));
            } catch (const std::exception&) {
                // Ignore ex, our custom message is more meaningful.
                THROW_ERROR_EXCEPTION("Failed to parse control attribute name %s",
                    ~Stroka(name).Quote());
            }
            ControlState = EControlState::ExpectControlAttributeValue;
            return;

        case EControlState::ExpectEndControlAttributes:
            YCHECK(Depth == 1);
            THROW_ERROR_EXCEPTION("Too many control attributes per record: at most one attribute is allowed");
            break;

        default:
            YUNREACHABLE();
    }

    YCHECK(Depth > 0);
    if (Depth == 1) {
        Offsets.push_back(RowBuffer.Size());
        RowBuffer.Write(name);

        if (RowBuffer.Size() > NTableClient::MaxRowWeightLimit) {
            THROW_ERROR_EXCEPTION(
                "Row weight is too large (%d > %" PRId64 ")",
                RowBuffer.Size(),
                NTableClient::MaxRowWeightLimit);
        }

        Offsets.push_back(RowBuffer.Size());
    } else {
        ValueWriter.OnKeyedItem(name);
    }
}

void TTableConsumer::OnEndMap()
{
    YCHECK(Depth > 0);
    // No control attribute allows map or composite values.
    YCHECK(ControlState == EControlState::None);

    --Depth;

    if (Depth > 0) {
        ValueWriter.OnEndMap();
        return;
    }

    YCHECK(Offsets.size() % 2 == 0);

    TRow row;
    row.reserve(Offsets.size() / 2);

    int index = Offsets.size();
    int begin = RowBuffer.Size();
    while (index > 0) {
        int end = begin;
        begin = Offsets[--index];
        TStringBuf value(RowBuffer.Begin() + begin, end - begin);

        end = begin;
        begin = Offsets[--index];
        TStringBuf name(RowBuffer.Begin() + begin, end - begin);

        row.push_back(std::make_pair(name, value));
    }

    Writer->WriteRow(row);

    Offsets.clear();
    RowBuffer.Clear();
}

void TTableConsumer::OnEndList()
{
   // No control attribute allow list or composite values.
    YCHECK(ControlState == EControlState::None);

    --Depth;
    YCHECK(Depth > 0);
    ValueWriter.OnEndList();
}

void TTableConsumer::OnEndAttributes()
{
    --Depth;

    switch (ControlState) {
        case EControlState::ExpectControlAttributeName:
            THROW_ERROR_EXCEPTION("Too few control attributes per record: at least one attribute is required");
            break;

        case EControlState::ExpectEndControlAttributes:
            YCHECK(Depth == 0);
            ControlState = EControlState::ExpectEntity;
            break;

        case EControlState::None:
            YCHECK(Depth > 0);
            ValueWriter.OnEndAttributes();
            break;

        default:
            YUNREACHABLE();
    };
}

void TTableConsumer::OnRaw(const TStringBuf& yson, EYsonType type)
{
    YCHECK(ControlState == EControlState::None);
    YCHECK(Depth > 0);
    YCHECK(type == EYsonType::Node);

    ValueWriter.OnRaw(yson, type);
}

void TTableConsumer::ThrowMapExpected() const
{
    ThrowError("Invalid row format, map expected");
}

void TTableConsumer::ThrowEntityExpected() const
{
    ThrowError("Invalid row format, there are control attributes, entity expected");
}

void TTableConsumer::ThrowInvalidControlAttribute(const Stroka& whatsWrong) const
{
    ThrowError(Format("Control attribute %Qlv cannot %v",
        ControlAttribute,
        whatsWrong));
}

void TTableConsumer::ThrowError(const Stroka& message) const
{
    THROW_ERROR_EXCEPTION(message)
        << TErrorAttribute("table_index", CurrentTableIndex)
        << TErrorAttribute("row_index", Writer->GetRowCount());
}

////////////////////////////////////////////////////////////////////////////////

TTableConsumerBase::TTableConsumerBase(
    const TTableSchema& schema,
    const TKeyColumns& keyColumns)
    : TreatMissingAsNull_(true)
    , AllowNonSchemaColumns_(true)
    , KeyColumnCount_(static_cast<int>(keyColumns.size()))
    , NameTable_(TNameTable::FromSchema(schema))
    , ControlState_(EControlState::None)
    , Depth_(0)
    , ColumnIndex_(0)
{
    SchemaColumnDescriptors_.resize(schema.Columns().size());
    for (const auto& column : schema.Columns()) {
        int id = NameTable_->GetId(column.Name);
        SchemaColumnDescriptors_[id].Type = column.Type;
    }
}

TNameTablePtr TTableConsumerBase::GetNameTable() const
{
    return NameTable_;
}

bool TTableConsumerBase::GetAllowNonSchemaColumns() const
{
    return AllowNonSchemaColumns_;
}

void TTableConsumerBase::SetAllowNonSchemaColumns(bool value)
{
    AllowNonSchemaColumns_ = value;
}

TError TTableConsumerBase::AttachLocationAttributes(TError error)
{
    return error;
}

void TTableConsumerBase::OnControlInt64Scalar(i64 /*value*/)
{
    ThrowControlAttributesNotSupported();
}

void TTableConsumerBase::OnControlStringScalar(const TStringBuf& /*value*/)
{
    ThrowControlAttributesNotSupported();
}

void TTableConsumerBase::OnStringScalar(const TStringBuf& value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        OnControlStringScalar(value);
        return;
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        WriteValue(MakeStringValue<TUnversionedValue>(value, ColumnIndex_));
    }
}

void TTableConsumerBase::OnInt64Scalar(i64 value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        OnControlInt64Scalar(value);
        return;
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        WriteValue(MakeInt64Value<TUnversionedValue>(value, ColumnIndex_));
    }
}

void TTableConsumerBase::OnDoubleScalar(double value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a double value");
        return;
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        WriteValue(MakeDoubleValue<TUnversionedValue>(value, ColumnIndex_));
    }
}

void TTableConsumerBase::OnBooleanScalar(bool value)
{
    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        WriteValue(MakeBooleanValue<TUnversionedValue>(value, ColumnIndex_));
    }
}

void TTableConsumerBase::OnEntity()
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
    } else {
        ThrowCompositesNotSupported();
    }
}

void TTableConsumerBase::OnBeginList()
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
        ++Depth_;
        ThrowCompositesNotSupported();
    }
}

void TTableConsumerBase::OnBeginAttributes()
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("have attributes");
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ControlState_ = EControlState::ExpectName;
    } else {
        ThrowCompositesNotSupported();
    }

    ++Depth_;
}

void TTableConsumerBase::ThrowControlAttributesNotSupported()
{
    THROW_ERROR AttachLocationAttributes(TError("Control attributes are not supported"));
}

void TTableConsumerBase::ThrowMapExpected()
{
    THROW_ERROR AttachLocationAttributes(TError("Invalid row format, map expected"));
}

void TTableConsumerBase::ThrowCompositesNotSupported()
{
    THROW_ERROR AttachLocationAttributes(TError("Composite types are not supported"));
}

void TTableConsumerBase::ThrowInvalidSchemaColumnType(int columnId, NVersionedTableClient::EValueType actualType)
{
    THROW_ERROR AttachLocationAttributes(TError("Invalid type of schema column %s: expected %s, actual %s",
        ~NameTable_->GetName(columnId).Quote(),
        ~FormatEnum(SchemaColumnDescriptors_[columnId].Type).Quote(),
        ~FormatEnum(actualType).Quote()));
}

void TTableConsumerBase::ThrowInvalidControlAttribute(const Stroka& whatsWrong)
{
    THROW_ERROR AttachLocationAttributes(TError("Control attribute %s cannot %s",
        ~FormatEnum(ControlAttribute_).Quote(),
        ~whatsWrong));
}

void TTableConsumerBase::OnListItem()
{
    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        // Row separator, do nothing.
    } else {
        ThrowCompositesNotSupported();
    }
}

void TTableConsumerBase::OnBeginMap()
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a map");
    }

    YASSERT(ControlState_ == EControlState::None);

    ++Depth_;
    if (Depth_ == 1) {
        OnBeginRow();
    } else {
        ThrowCompositesNotSupported();
    }
}

void TTableConsumerBase::OnKeyedItem(const TStringBuf& name)
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
                THROW_ERROR AttachLocationAttributes(TError("Failed to parse control attribute name %s",
                    ~Stroka(name).Quote()));
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
        if (AllowNonSchemaColumns_) {
            ColumnIndex_ = NameTable_->GetIdOrRegisterName(name);
        } else {
            auto maybeIndex = NameTable_->FindId(name);
            if (!maybeIndex) {
                THROW_ERROR AttachLocationAttributes(TError("No such column %s in schema",
                    ~Stroka(name).Quote()));
            }
            ColumnIndex_ = *maybeIndex;
        }
    } else {
        ThrowCompositesNotSupported();
    }
}

void TTableConsumerBase::OnEndMap()
{
    YASSERT(Depth_ > 0);
    // No control attribute allows map or composite values.
    YASSERT(ControlState_ == EControlState::None);

    --Depth_;
    if (Depth_ > 0) {
        ThrowCompositesNotSupported();
    } else {
        for (int id = 0; id < static_cast<int>(SchemaColumnDescriptors_.size()); ++id) {
            if (SchemaColumnDescriptors_[id].Written) {
                SchemaColumnDescriptors_[id].Written = false;
            } else {
                if (id >= KeyColumnCount_ && TreatMissingAsNull_) {
                    OnValue(MakeSentinelValue<TUnversionedValue>(EValueType::Null, id));
                }
            }
        }
        OnEndRow();
    }
}

void TTableConsumerBase::OnEndList()
{
    // No control attribute allow list or composite values.
    YASSERT(ControlState_ == EControlState::None);

    --Depth_;
    YASSERT(Depth_ > 0);
    ThrowCompositesNotSupported();
}

void TTableConsumerBase::OnEndAttributes()
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
            ThrowCompositesNotSupported();
            break;

        default:
            YUNREACHABLE();
    }
}

void TTableConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    YUNREACHABLE();
}

void TTableConsumerBase::WriteValue(const TUnversionedValue& value)
{
    int id = value.Id;
    if (id < SchemaColumnDescriptors_.size()) {
        auto type = NVersionedTableClient::EValueType(value.Type);
        auto& descriptor = SchemaColumnDescriptors_[id];
        if (type != descriptor.Type) {
            ThrowInvalidSchemaColumnType(id, type);
        }
        descriptor.Written = true;
    }
    OnValue(value);
    ColumnIndex_ = -1;
}

////////////////////////////////////////////////////////////////////////////////

TBuildingTableConsumer::TBuildingTableConsumer(
    const TTableSchema& schema,
    const TKeyColumns& keyColumns)
    : TTableConsumerBase(schema, keyColumns)
    , RowIndex_(0)
{ }

const std::vector<TUnversionedOwningRow>& TBuildingTableConsumer::Rows() const
{
    return Rows_;
}

bool TBuildingTableConsumer::GetTreatMissingAsNull() const
{
    return TreatMissingAsNull_;
}

void TBuildingTableConsumer::SetTreatMissingAsNull(bool value)
{
    TreatMissingAsNull_ = value;
}

TError TBuildingTableConsumer::AttachLocationAttributes(TError error)
{
    return error << TErrorAttribute("row_index", RowIndex_);
}

void TBuildingTableConsumer::OnBeginRow()
{ }

void TBuildingTableConsumer::OnValue(const TUnversionedValue& value)
{
    Builder_.AddValue(value);
}

void TBuildingTableConsumer::OnEndRow()
{
    std::sort(
        Builder_.BeginValues(),
        Builder_.EndValues(),
        [] (const TUnversionedValue& lhs, const TUnversionedValue& rhs) {
            return lhs.Id < rhs.Id;
        });
    Rows_.emplace_back(Builder_.GetRowAndReset());
    ++RowIndex_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
