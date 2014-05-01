#include "stdafx.h"
#include "schemed_dsv_writer.h"

#include <core/misc/error.h>

#include <core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

TSchemedDsvConsumer::TSchemedDsvConsumer(
        TOutputStream* stream,
        TSchemedDsvFormatConfigPtr config)
    : Stream_(stream)
    , Config_(config)
    , Table_(Config_)
    , Keys_(Config_->Columns.begin(), Config_->Columns.end())
    , ValueCount_(0)
    , TableIndex_(0)
    , State_(EState::None)
{
    // Initialize Values_ with alive keys
    for (const auto& key: Keys_) {
        Values_[key] = TStringBuf();
    }
}

void TSchemedDsvConsumer::OnDoubleScalar(double value)
{
    if (State_ == EState::None) {
        return;         
    }

    if (State_ == EState::ExpectValue) {
        ValueHolder_.push_back(::ToString(value));
        Values_[CurrentKey_] = ValueHolder_.back();
        State_ = EState::None;
        ValueCount_ += 1;
    } else {
        YCHECK(State_ == EState::None);
    }
}

void TSchemedDsvConsumer::OnBeginList()
{
    THROW_ERROR_EXCEPTION("Lists are not supported by schemed DSV");
}

void TSchemedDsvConsumer::OnListItem()
{
    YASSERT(State_ == EState::None);
}

void TSchemedDsvConsumer::OnEndList()
{
    YUNREACHABLE();
}

void TSchemedDsvConsumer::OnBeginAttributes()
{
    if (State_ == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Attributes are not supported by schemed DSV");
    }

    YASSERT(State_ == EState::None);
    State_ = EState::ExpectAttributeName;
}

void TSchemedDsvConsumer::OnEndAttributes()
{
    YASSERT(State_ == EState::ExpectEndAttributes);
    State_ = EState::ExpectEntity;
}

void TSchemedDsvConsumer::OnBeginMap()
{
    if (State_ == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by schemed DSV");
    }
    YASSERT(State_ == EState::None);
}

void TSchemedDsvConsumer::OnEntity()
{
    if (State_ == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Entities are not supported by schemed DSV");
    }

    YASSERT(State_ == EState::ExpectEntity);
    State_ = EState::None;
}

void TSchemedDsvConsumer::OnIntegerScalar(i64 value)
{
    if (State_ == EState::None) {
        return;         
    }

    if (State_ == EState::ExpectValue) {
        ValueHolder_.push_back(::ToString(value));
        Values_[CurrentKey_] = ValueHolder_.back();
        State_ = EState::None;
        ValueCount_ += 1;
        return;
    }

    YASSERT(State_ == EState::ExpectAttributeValue);

    switch (ControlAttribute_) {
    case EControlAttribute::TableIndex:
        TableIndex_ = value;
        break;

    default:
        YUNREACHABLE();
    }

    State_ = EState::ExpectEndAttributes;
}

void TSchemedDsvConsumer::OnStringScalar(const TStringBuf& value)
{
    if (State_ == EState::ExpectValue) {
        Values_[CurrentKey_] = value;
        State_ = EState::None;
        ValueCount_ += 1;
    } else {
        YCHECK(State_ == EState::None);
    }
}

void TSchemedDsvConsumer::OnKeyedItem(const TStringBuf& key)
{
    if (State_ ==  EState::ExpectAttributeName) {
        ControlAttribute_ = ParseEnum<EControlAttribute>(ToString(key));
        State_ = EState::ExpectAttributeValue;
    } else {
        YASSERT(State_ == EState::None);
        if (Keys_.find(key) != Keys_.end()) {
            CurrentKey_ = key;
            State_ = EState::ExpectValue;
        }
    }
}

void TSchemedDsvConsumer::OnEndMap()
{
    YASSERT(State_ == EState::None);

    WriteRow();
}

void TSchemedDsvConsumer::WriteRow()
{
    typedef TSchemedDsvFormatConfig::EMissingValueMode EMissingValueMode;

    if (ValueCount_ != Keys_.size() && Config_->MissingValueMode == EMissingValueMode::Fail) {
        THROW_ERROR_EXCEPTION("Some column is missing in row");
    }

    if (ValueCount_ == Keys_.size() || Config_->MissingValueMode == EMissingValueMode::PrintSentinel) {
        if (Config_->EnableTableIndex) {
            Stream_->Write(ToString(TableIndex_));
            Stream_->Write(Config_->FieldSeparator);
        }
        for (int i = 0; i < Keys_.size(); ++i) {
            auto key = Config_->Columns[i];
            TStringBuf value = Values_[key];
            if (!value.IsInited()) {
                value = Config_->MissingValueSentinel;
            }
            EscapeAndWrite(value);
            Stream_->Write(
                i + 1 < Keys_.size()
                ? Config_->FieldSeparator
                : Config_->RecordSeparator);
        }
    }

    // Clear row
    ValueCount_ = 0;
    ValueHolder_.clear();
    if (Config_->MissingValueMode == EMissingValueMode::PrintSentinel) {
        for (const auto& key: Keys_) {
            Values_[key] = TStringBuf();
        }
    }
}

void TSchemedDsvConsumer::EscapeAndWrite(const TStringBuf& value) const
{
    if (Config_->EnableEscaping) {
        WriteEscaped(
            Stream_,
            value,
            Table_.Stops,
            Table_.Escapes,
            Config_->EscapingSymbol);
    } else {
        Stream_->Write(value);
    }
}

////////////////////////////////////////////////////////////////////////////////

static auto PresetResult = MakeFuture(TError());

TSchemafulDsvWriter::TSchemafulDsvWriter(
    IAsyncOutputStreamPtr stream,
    TSchemedDsvFormatConfigPtr config)
    : Stream_(stream)
    , Config_(config)
{ }

TAsyncError TSchemafulDsvWriter::Open(
    const TTableSchema& schema,
    const TNullable<TKeyColumns>& /*keyColumns*/)
{
    for (const auto& name : Config_->Columns) {
        int id;
        try {
            id = schema.GetColumnIndexOrThrow(name);
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
        }
        ColumnIdMapping_.push_back(id);
    }

    return PresetResult;
}

TAsyncError TSchemafulDsvWriter::Close()
{
    return PresetResult;
}

bool TSchemafulDsvWriter::Write(const std::vector<TUnversionedRow>& rows)
{
    // TODO(babenko): handle escaping properly
    Buffer_.Clear();

    auto idMappingBegin = ColumnIdMapping_.begin();
    auto idMappingEnd = ColumnIdMapping_.end();
    for (auto row : rows) {
        for (auto idMappingCurrent = idMappingBegin; idMappingCurrent != idMappingEnd; ++idMappingCurrent) {
            int id = *idMappingCurrent;
            WriteValue(row[id]);
            if (idMappingCurrent != idMappingEnd) {
                WriteRaw(STRINGBUF("\t"));
            }
        }
        WriteRaw(STRINGBUF("\n"));
    }

    return Stream_->Write(Buffer_.Begin(), Buffer_.Size());
}

TAsyncError TSchemafulDsvWriter::GetReadyEvent()
{
    return Stream_->GetReadyEvent();
}

void TSchemafulDsvWriter::WriteValue(const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Integer: {
            char buffer[64];
            int length = sprintf(buffer, "%" PRId64, value.Data.Integer);
            WriteRaw(TStringBuf(buffer, length));
            break;
        }

        case EValueType::String:
            WriteRaw(TStringBuf(value.Data.String, value.Length));
            break;

        default:
            // TODO(babenko): improve
            WriteRaw(STRINGBUF("???"));
            break;
    }
}

void TSchemafulDsvWriter::WriteRaw(const TStringBuf& str)
{
    Buffer_.Append(str.begin(), str.length());
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NFormats
} // namespace NYT
