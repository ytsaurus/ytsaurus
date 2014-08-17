#include "stdafx.h"
#include "schemaful_dsv_writer.h"

#include <core/misc/error.h>

#include <core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

TSchemafulDsvConsumer::TSchemafulDsvConsumer(
    TOutputStream* stream,
    TSchemafulDsvFormatConfigPtr config)
    : Stream_(stream)
    , Config_(config)
    , Table_(Config_)
{
    const auto& columns = Config_->GetColumnsOrThrow();
    Keys_.insert(columns.begin(), columns.end());

    // Initialize Values_ with alive keys.
    for (const auto& key: Keys_) {
        Values_[key] = TStringBuf();
    }
}

void TSchemafulDsvConsumer::OnUint64Scalar(ui64 value)
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

void TSchemafulDsvConsumer::OnDoubleScalar(double value)
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

void TSchemafulDsvConsumer::OnBooleanScalar(bool value)
{
    if (State_ == EState::None) {
        return;
    }

    if (State_ == EState::ExpectValue) {
        ValueHolder_.push_back(Stroka(FormatBool(value)));
        Values_[CurrentKey_] = ValueHolder_.back();
        State_ = EState::None;
        ValueCount_ += 1;
    } else {
        YCHECK(State_ == EState::None);
    }
}

void TSchemafulDsvConsumer::OnBeginList()
{
    THROW_ERROR_EXCEPTION("Lists are not supported by schemaful DSV");
}

void TSchemafulDsvConsumer::OnListItem()
{
    YASSERT(State_ == EState::None);
}

void TSchemafulDsvConsumer::OnEndList()
{
    YUNREACHABLE();
}

void TSchemafulDsvConsumer::OnBeginAttributes()
{
    if (State_ == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Attributes are not supported by schemaful DSV");
    }

    YASSERT(State_ == EState::None);
    State_ = EState::ExpectAttributeName;
}

void TSchemafulDsvConsumer::OnEndAttributes()
{
    YASSERT(State_ == EState::ExpectEndAttributes);
    State_ = EState::ExpectEntity;
}

void TSchemafulDsvConsumer::OnBeginMap()
{
    if (State_ == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by schemaful DSV");
    }
    YASSERT(State_ == EState::None);
}

void TSchemafulDsvConsumer::OnEntity()
{
    if (State_ == EState::ExpectValue) {
        THROW_ERROR_EXCEPTION("Entities are not supported by schemaful DSV");
    }

    YASSERT(State_ == EState::ExpectEntity);
    State_ = EState::None;
}

void TSchemafulDsvConsumer::OnInt64Scalar(i64 value)
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

void TSchemafulDsvConsumer::OnStringScalar(const TStringBuf& value)
{
    if (State_ == EState::ExpectValue) {
        Values_[CurrentKey_] = value;
        State_ = EState::None;
        ValueCount_ += 1;
    } else {
        YCHECK(State_ == EState::None);
    }
}

void TSchemafulDsvConsumer::OnKeyedItem(const TStringBuf& key)
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

void TSchemafulDsvConsumer::OnEndMap()
{
    YASSERT(State_ == EState::None);

    WriteRow();
}

void TSchemafulDsvConsumer::WriteRow()
{
    typedef TSchemafulDsvFormatConfig::EMissingValueMode EMissingValueMode;

    if (ValueCount_ != Keys_.size() && Config_->MissingValueMode == EMissingValueMode::Fail) {
        THROW_ERROR_EXCEPTION("Some column is missing in row");
    }

    if (ValueCount_ == Keys_.size() || Config_->MissingValueMode == EMissingValueMode::PrintSentinel) {
        if (Config_->EnableTableIndex) {
            Stream_->Write(ToString(TableIndex_));
            Stream_->Write(Config_->FieldSeparator);
        }
        for (int i = 0; i < Keys_.size(); ++i) {
            auto key = (*Config_->Columns)[i];
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

void TSchemafulDsvConsumer::EscapeAndWrite(const TStringBuf& value) const
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

TSchemafulDsvWriter::TSchemafulDsvWriter(
    IAsyncOutputStreamPtr stream,
    TSchemafulDsvFormatConfigPtr config)
    : Stream_(stream)
    , Config_(config)
{ }

TAsyncError TSchemafulDsvWriter::Open(
    const TTableSchema& schema,
    const TNullable<TKeyColumns>& /*keyColumns*/)
{
    if (Config_->Columns) {
        for (const auto& name : *Config_->Columns) {
            int id;
            try {
                id = schema.GetColumnIndexOrThrow(name);
            } catch (const std::exception& ex) {
                return MakeFuture(TError(ex));
            }
            ColumnIdMapping_.push_back(id);
        }
    } else {
        for (int id = 0; id < schema.Columns().size(); ++id) {
            ColumnIdMapping_.push_back(id);
        }
    }
    return OKFuture;
}

TAsyncError TSchemafulDsvWriter::Close()
{
    return OKFuture;
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
                WriteRaw('\t');
            }
        }
        WriteRaw('\n');
    }

    Result_ = Stream_->Write(Buffer_.Begin(), Buffer_.Size());
    return Result_.IsSet() && Result_.Get().IsOK();
}

TAsyncError TSchemafulDsvWriter::GetReadyEvent()
{
    return Result_;
}

static ui16 DigitPairs[100] = {
    12336,  12337,  12338,  12339,  12340,  12341,  12342,  12343,  12344,  12345,
    12592,  12593,  12594,  12595,  12596,  12597,  12598,  12599,  12600,  12601,
    12848,  12849,  12850,  12851,  12852,  12853,  12854,  12855,  12856,  12857,
    13104,  13105,  13106,  13107,  13108,  13109,  13110,  13111,  13112,  13113,
    13360,  13361,  13362,  13363,  13364,  13365,  13366,  13367,  13368,  13369,
    13616,  13617,  13618,  13619,  13620,  13621,  13622,  13623,  13624,  13625,
    13872,  13873,  13874,  13875,  13876,  13877,  13878,  13879,  13880,  13881,
    14128,  14129,  14130,  14131,  14132,  14133,  14134,  14135,  14136,  14137,
    14384,  14385,  14386,  14387,  14388,  14389,  14390,  14391,  14392,  14393,
    14640,  14641,  14642,  14643,  14644,  14645,  14646,  14647,  14648,  14649
};

char* TSchemafulDsvWriter::WriteInt64Reversed(char* ptr, i64 value)
{
    if (value == 0) {
        *ptr++ = '0';
        return ptr;
    }

    bool negative = false;
    if (value < 0) {
        negative = true;
        value = -value;
    }

    while (value >= 10) {
        i64 rem = value % 100;
        i64 quot = value / 100;
        *reinterpret_cast<ui16*>(ptr) = DigitPairs[rem];
        ptr += 2;
        value = quot;
    }

    if (value > 0) {
        *ptr++ = ('0' + value);
    }

    if (negative) {
        *ptr++ = '-';
    }

    return ptr;
}

char* TSchemafulDsvWriter::WriteUint64Reversed(char* ptr, ui64 value)
{
    if (value == 0) {
        *ptr++ = '0';
        return ptr;
    }

    while (value >= 10) {
        i64 rem = value % 100;
        i64 quot = value / 100;
        *reinterpret_cast<ui16*>(ptr) = DigitPairs[rem];
        ptr += 2;
        value = quot;
    }

    if (value > 0) {
        *ptr++ = ('0' + value);
    }

    return ptr;
}

void TSchemafulDsvWriter::WriteValue(const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Null:
            break;

        case EValueType::Int64:
        case EValueType::Uint64: {
            char buf[64];
            char* begin = buf;
            char* end = EValueType::Int64
                ? WriteInt64Reversed(begin, value.Data.Int64)
                : WriteUint64Reversed(begin, value.Data.Uint64);
            size_t length = end - begin;

            Buffer_.Resize(Buffer_.Size() + length, false);
            char* src = begin;
            char* dst = Buffer_.End() - 1;
            while (src != end) {
                *dst-- = *src++;
            }
            break;
        }

        case EValueType::Double: {
            // TODO(babenko): optimize
            const size_t maxSize = 64;
            Buffer_.Resize(Buffer_.Size() + maxSize);
            int size = sprintf(Buffer_.End() - maxSize, "%lf", value.Data.Double);
            Buffer_.Resize(Buffer_.Size() - maxSize + size);
            break;
        }

        case EValueType::Boolean: {
            WriteRaw(FormatBool(value.Data.Boolean));
            break;
        }

        case EValueType::String:
            WriteRaw(TStringBuf(value.Data.String, value.Length));
            break;

        default:
            WriteRaw('?');
            break;
    }
}

void TSchemafulDsvWriter::WriteRaw(const TStringBuf& str)
{
    Buffer_.Append(str.begin(), str.length());
}

void TSchemafulDsvWriter::WriteRaw(char ch)
{
    Buffer_.Append(ch);
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NFormats
} // namespace NYT
