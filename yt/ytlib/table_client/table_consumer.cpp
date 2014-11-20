#include "stdafx.h"
#include "sync_writer.h"
#include "table_consumer.h"
#include "config.h"

#include <core/misc/string.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/schemaless_writer.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NYson;
using namespace NVersionedTableClient;

const i64 MaxValueBufferSize = 10 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

void TLegacyTableConsumer::OnStringScalar(const TStringBuf& value)
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a string value");
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnStringScalar(value);
    }
}

void TLegacyTableConsumer::OnInt64Scalar(i64 value)
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);

        switch (ControlAttribute) {
            case EControlAttribute::TableIndex: {
                if (value < 0 || value >= Writers.size()) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid table index: expected in range [0, %v], actual %v",
                        Writers.size() - 1,
                        value)
                        << TErrorAttribute("row_index", Writer->GetRowCount());
                }
                CurrentTableIndex = value;
                Writer = Writers[CurrentTableIndex];
                ControlState = ELegacyTableConsumerControlState::ExpectEndControlAttributes;
                break;
            }

            default:
                ThrowInvalidControlAttribute("be an integer value");
        }

        return;
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnInt64Scalar(value);
    }
}

void TLegacyTableConsumer::OnUint64Scalar(ui64 value)
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a uint64 value");
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnUint64Scalar(value);
    }
}

void TLegacyTableConsumer::OnDoubleScalar(double value)
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a double value");
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnDoubleScalar(value);
    }
}

void TLegacyTableConsumer::OnBooleanScalar(bool value)
{
    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter.OnBooleanScalar(value);
    }
}

void TLegacyTableConsumer::OnEntity()
{
    switch (ControlState) {
        case ELegacyTableConsumerControlState::None:
            break;

        case ELegacyTableConsumerControlState::ExpectEntity:
	        YCHECK(Depth == 0);
            // Successfully processed control statement.
            ControlState = ELegacyTableConsumerControlState::None;
            return;

	    case ELegacyTableConsumerControlState::ExpectControlAttributeValue:
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

void TLegacyTableConsumer::OnBeginList()
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a list");
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ThrowMapExpected();
    } else {
        ++Depth;
        ValueWriter.OnBeginList();
    }
}

void TLegacyTableConsumer::OnBeginAttributes()
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("have attributes");
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        ControlState = ELegacyTableConsumerControlState::ExpectControlAttributeName;
    } else {
        ValueWriter.OnBeginAttributes();
    }

    ++Depth;
}

void TLegacyTableConsumer::OnListItem()
{
    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth == 0) {
        // Row separator, do nothing.
    } else {
        ValueWriter.OnListItem();
    }
}

void TLegacyTableConsumer::OnBeginMap()
{
    if (ControlState == ELegacyTableConsumerControlState::ExpectControlAttributeValue) {
        YCHECK(Depth == 1);
        ThrowInvalidControlAttribute("be a map");
    }

    if (ControlState == ELegacyTableConsumerControlState::ExpectEntity) {
        ThrowEntityExpected();
    }

    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    if (Depth > 0) {
        ValueWriter.OnBeginMap();
    }

    ++Depth;
}

void TLegacyTableConsumer::OnKeyedItem(const TStringBuf& name)
{
    switch (ControlState) {
   	    case ELegacyTableConsumerControlState::None:
            break;

        case ELegacyTableConsumerControlState::ExpectControlAttributeName:
            YCHECK(Depth == 1);
            try {
                ControlAttribute = ParseEnum<EControlAttribute>(ToString(name));
            } catch (const std::exception&) {
                // Ignore ex, our custom message is more meaningful.
                THROW_ERROR_EXCEPTION("Failed to parse control attribute name %Qv",
                    name);
            }
            ControlState = ELegacyTableConsumerControlState::ExpectControlAttributeValue;
            return;

        case ELegacyTableConsumerControlState::ExpectEndControlAttributes:
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
                "Row weight is too large (%v > %v)",
                RowBuffer.Size(),
                NTableClient::MaxRowWeightLimit);
        }

        Offsets.push_back(RowBuffer.Size());
    } else {
        ValueWriter.OnKeyedItem(name);
    }
}

void TLegacyTableConsumer::OnEndMap()
{
    YCHECK(Depth > 0);
    // No control attribute allows map or composite values.
    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

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

void TLegacyTableConsumer::OnEndList()
{
   // No control attribute allow list or composite values.
    YCHECK(ControlState == ELegacyTableConsumerControlState::None);

    --Depth;
    YCHECK(Depth > 0);
    ValueWriter.OnEndList();
}

void TLegacyTableConsumer::OnEndAttributes()
{
    --Depth;

    switch (ControlState) {
        case ELegacyTableConsumerControlState::ExpectControlAttributeName:
            THROW_ERROR_EXCEPTION("Too few control attributes per record: at least one attribute is required");
            break;

        case ELegacyTableConsumerControlState::ExpectEndControlAttributes:
            YCHECK(Depth == 0);
            ControlState = ELegacyTableConsumerControlState::ExpectEntity;
            break;

        case ELegacyTableConsumerControlState::None:
            YCHECK(Depth > 0);
            ValueWriter.OnEndAttributes();
            break;

        default:
            YUNREACHABLE();
    };
}

void TLegacyTableConsumer::OnRaw(const TStringBuf& yson, EYsonType type)
{
    YCHECK(ControlState == ELegacyTableConsumerControlState::None);
    YCHECK(Depth > 0);
    YCHECK(type == EYsonType::Node);

    ValueWriter.OnRaw(yson, type);
}

void TLegacyTableConsumer::ThrowMapExpected() const
{
    ThrowError("Invalid row format, map expected");
}

void TLegacyTableConsumer::ThrowEntityExpected() const
{
    ThrowError("Invalid row format, there are control attributes, entity expected");
}

void TLegacyTableConsumer::ThrowInvalidControlAttribute(const Stroka& whatsWrong) const
{
    ThrowError(Format("Control attribute %Qlv cannot %v",
        ControlAttribute,
        whatsWrong));
}

void TLegacyTableConsumer::ThrowError(const Stroka& message) const
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
    , ValueWriter_(&ValueBuffer_)
    , Depth_(0)
    , ColumnIndex_(0)
{
    if (schema.Columns().empty()) {
        SchemaColumnDescriptors_.resize(keyColumns.size());
        for (const auto& name : keyColumns) {
            int id = NameTable_->GetId(name);
            SchemaColumnDescriptors_[id].Type = EValueType::TheBottom;
        }
    } else {
        // ToDo(psushin): validate key columns is schema prefix.
        SchemaColumnDescriptors_.resize(schema.Columns().size());
        for (const auto& column : schema.Columns()) {
            int id = NameTable_->GetId(column.Name);
            SchemaColumnDescriptors_[id].Type = column.Type;
        }
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
    if (ControlState_ == ETableConsumerControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        OnControlStringScalar(value);
        return;
    }

    YASSERT(ControlState_ == ETableConsumerControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else if (Depth_ == 1) {
        WriteValue(MakeUnversionedStringValue(value, ColumnIndex_));
    } else {
        ValueWriter_.OnStringScalar(value);
    }
}

void TTableConsumerBase::OnInt64Scalar(i64 value)
{
    if (ControlState_ == ETableConsumerControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        OnControlInt64Scalar(value);
        return;
    }

    YASSERT(ControlState_ == ETableConsumerControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else if (Depth_ == 1) {
        WriteValue(MakeUnversionedInt64Value(value, ColumnIndex_));
    } else {
        ValueWriter_.OnInt64Scalar(value);
    }
}

void TTableConsumerBase::OnUint64Scalar(ui64 value)
{
    if (ControlState_ == EControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a uint64 value");
        return;
    }

    YASSERT(ControlState_ == EControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        WriteValue(MakeUint64Value<TUnversionedValue>(value, ColumnIndex_));
    }
}

void TTableConsumerBase::OnDoubleScalar(double value)
{
    if (ControlState_ == ETableConsumerControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a double value");
        return;
    }

    YASSERT(ControlState_ == ETableConsumerControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else if (Depth_ == 1) {
        WriteValue(MakeUnversionedDoubleValue(value, ColumnIndex_));
    } else {
        ValueWriter_.OnDoubleScalar(value);
    }
}

void TTableConsumerBase::OnBooleanScalar(bool value)
{
    YASSERT(ControlState_ == ETableConsumerControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        WriteValue(MakeBooleanValue<TUnversionedValue>(value, ColumnIndex_));
    }
}

void TTableConsumerBase::OnEntity()
{
    switch (ControlState_) {
        case ETableConsumerControlState::None:
            break;

        case ETableConsumerControlState::ExpectEntity:
            YASSERT(Depth_ == 0);
            // Successfully processed control statement.
            ControlState_ = ETableConsumerControlState::None;
            return;

        case ETableConsumerControlState::ExpectValue:
            ThrowInvalidControlAttribute("be an entity");
            break;

        default:
            YUNREACHABLE();
    }


    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        ValueWriter_.OnEntity();
    }
}

void TTableConsumerBase::OnBeginList()
{
    if (ControlState_ == ETableConsumerControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a list");
        return;
    }

    YASSERT(ControlState_ == ETableConsumerControlState::None);

    if (Depth_ == 0) {
        ThrowMapExpected();
    } else {
        if (Depth_ == 1) {
            ValueBegin_ = ValueBuffer_.Begin() + ValueBuffer_.Size();
        }
        ValueWriter_.OnBeginList();
    }
    ++Depth_;
}

void TTableConsumerBase::OnBeginAttributes()
{
    if (ControlState_ == ETableConsumerControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("have attributes");
    }

    YASSERT(ControlState_ == ETableConsumerControlState::None);

    if (Depth_ == 0) {
        ControlState_ = ETableConsumerControlState::ExpectName;
    } else {
        if (Depth_ == 1) {
            ValueBegin_ = ValueBuffer_.Begin() + ValueBuffer_.Size();
        }
        ValueWriter_.OnBeginAttributes();
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
    THROW_ERROR AttachLocationAttributes(TError("Invalid type of schema column %Qv: expected %Qlv, actual %Qlv",
        NameTable_->GetName(columnId),
        SchemaColumnDescriptors_[columnId].Type,
        actualType));
}

void TTableConsumerBase::ThrowInvalidControlAttribute(const Stroka& whatsWrong)
{
    THROW_ERROR AttachLocationAttributes(TError("Control attribute %Qlv cannot %v",
        ControlAttribute_,
        whatsWrong));
}

void TTableConsumerBase::OnListItem()
{
    YASSERT(ControlState_ == ETableConsumerControlState::None);

    if (Depth_ == 0) {
        // Row separator, do nothing.
    } else {
        ValueWriter_.OnListItem();
    }
}

void TTableConsumerBase::OnBeginMap()
{
    if (ControlState_ == ETableConsumerControlState::ExpectValue) {
        YASSERT(Depth_ == 1);
        ThrowInvalidControlAttribute("be a map");
    }

    YASSERT(ControlState_ == ETableConsumerControlState::None);

    if (Depth_ == 0) {
        OnBeginRow();
    } else {
        if (Depth_ == 1) {
            ValueBegin_ = ValueBuffer_.Begin() + ValueBuffer_.Size();
        }
        ValueWriter_.OnBeginMap();
    }
    ++Depth_;
}

void TTableConsumerBase::OnKeyedItem(const TStringBuf& name)
{
    switch (ControlState_) {
        case ETableConsumerControlState::None:
            break;

        case ETableConsumerControlState::ExpectName:
            YASSERT(Depth_ == 1);
            try {
                ControlAttribute_ = ParseEnum<EControlAttribute>(ToString(name));
            } catch (const std::exception&) {
                // Ignore ex, our custom message is more meaningful.
                THROW_ERROR AttachLocationAttributes(TError("Failed to parse control attribute name %Qv",
                    name));
            }
            ControlState_ = ETableConsumerControlState::ExpectValue;
            return;

        case ETableConsumerControlState::ExpectEndAttributes:
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
                THROW_ERROR AttachLocationAttributes(TError("No such column %Qv in schema",
                    name));
            }
            ColumnIndex_ = *maybeIndex;
        }
    } else {
        ValueWriter_.OnKeyedItem(name);
    }
}

void TTableConsumerBase::OnEndMap()
{
    YASSERT(Depth_ > 0);
    // No control attribute allows map or composite values.
    YASSERT(ControlState_ == ETableConsumerControlState::None);

    --Depth_;
    if (Depth_ > 0) {
        ValueWriter_.OnEndMap();
        if (Depth_ == 1) {
            OnValue(MakeUnversionedAnyValue(
                TStringBuf(ValueBegin_, ValueBuffer_.Begin() + ValueBuffer_.Size()),
                ColumnIndex_));
        }
    } else {
        for (int id = 0; id < static_cast<int>(SchemaColumnDescriptors_.size()); ++id) {
            if (SchemaColumnDescriptors_[id].Written) {
                SchemaColumnDescriptors_[id].Written = false;
            } else {
                if (id >= KeyColumnCount_ && TreatMissingAsNull_) {
                    OnValue(MakeUnversionedSentinelValue(EValueType::Null, id));
                }
            }
        }
        OnEndRow();
    }
}

void TTableConsumerBase::OnEndList()
{
    // No control attribute allow list or composite values.
    YASSERT(ControlState_ == ETableConsumerControlState::None);

    --Depth_;
    YASSERT(Depth_ > 0);

    ValueWriter_.OnEndList();
    if (Depth_ == 1) {
        OnValue(MakeUnversionedAnyValue(
            TStringBuf(ValueBegin_, ValueBuffer_.Begin() + ValueBuffer_.Size()),
            ColumnIndex_));
    }
}

void TTableConsumerBase::OnEndAttributes()
{
    --Depth_;

    switch (ControlState_) {
        case ETableConsumerControlState::ExpectName:
            THROW_ERROR AttachLocationAttributes(TError("Too few control attributes per record: at least one attribute is required"));
            break;

        case ETableConsumerControlState::ExpectEndAttributes:
            YASSERT(Depth_ == 0);
            ControlState_ = ETableConsumerControlState::ExpectEntity;
            break;

        case ETableConsumerControlState::None:
            YASSERT(Depth_ > 0);
            ValueWriter_.OnEndAttributes();
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
        auto type = EValueType(value.Type);
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
    Rows_.emplace_back(Builder_.FinishRow());
    ++RowIndex_;
}

////////////////////////////////////////////////////////////////////////////////

TWritingTableConsumer::TWritingTableConsumer()
    : TTableConsumerBase(TTableSchema(), TKeyColumns())
    , RowIndex_(0)
    , TableIndex_(0)
    , CurrentSize_(0)
{
    TreatMissingAsNull_ = false;
}

void TWritingTableConsumer::AddWriters(const std::vector<NVersionedTableClient::ISchemalessWriterPtr>& writers)
{
    for (const auto& writer : writers) {
        AddWriter(writer);
    }
}

void TWritingTableConsumer::AddWriter(ISchemalessWriterPtr writer)
{
    Writers_.push_back(writer);
    if (!CurrentWriter_) {
        CurrentWriter_ = writer;
    }
}

void TWritingTableConsumer::Flush()
{
    YCHECK(CurrentWriter_);

    if (!CurrentWriter_->Write(Rows_)) {
        auto error = WaitFor(CurrentWriter_->GetReadyEvent());
        if (!error.IsOK()) {
            THROW_ERROR_EXCEPTION("Table writer failed (TableIndex: %d)", TableIndex_) << error;
        }
    }

    Rows_.clear();
    OwningRows_.clear();
    CurrentSize_ = 0;
}

void TWritingTableConsumer::SetTableIndex(int tableIndex)
{
    TableIndex_ = tableIndex;
}

TError TWritingTableConsumer::AttachLocationAttributes(TError error)
{
    return error << TErrorAttribute("row_index", RowIndex_);
}

void TWritingTableConsumer::OnBeginRow()
{ }

void TWritingTableConsumer::OnValue(const TUnversionedValue& value)
{
    Builder_.AddValue(value);
}

void TWritingTableConsumer::OnEndRow()
{
    OwningRows_.emplace_back(Builder_.FinishRow());
    const auto& row = OwningRows_.back();

    CurrentSize_ += row.GetSize();
    Rows_.emplace_back(row.Get());

    ++RowIndex_;

    if (CurrentSize_ > MaxValueBufferSize) {
        Flush();
    }
}

void TWritingTableConsumer::OnControlInt64Scalar(i64 value)
{
    YCHECK(ControlAttribute_ == EControlAttribute::TableIndex);

    if (value >= Writers_.size()) {
        THROW_ERROR_EXCEPTION(
            "Invalid table index (Value: " PRId64 ", WriterCount: %d)",
            value,
            static_cast<int>(Writers_.size()));
    }

    Flush();

    TableIndex_ = value;
    CurrentWriter_ = Writers_[TableIndex_];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
