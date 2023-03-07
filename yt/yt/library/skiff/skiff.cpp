#include "skiff.h"

#include "skiff_validator.h"

#include <yt/core/misc/error.h>

#include <util/system/unaligned_mem.h>

namespace NYT::NSkiff {

////////////////////////////////////////////////////////////////////////////////

TUncheckedSkiffParser::TUncheckedSkiffParser(IZeroCopyInput* underlying)
    : Underlying_(underlying)
    , Buffer_(512_KB)
{ }

TUncheckedSkiffParser::TUncheckedSkiffParser(const TSkiffSchemaPtr& /*schema*/, IZeroCopyInput* underlying)
    : TUncheckedSkiffParser(underlying)
{ }

i64 TUncheckedSkiffParser::ParseInt64()
{
    return ParseSimple<i64>();
}

ui64 TUncheckedSkiffParser::ParseUint64()
{
    return ParseSimple<ui64>();
}

double TUncheckedSkiffParser::ParseDouble()
{
    return ParseSimple<double>();
}

bool TUncheckedSkiffParser::ParseBoolean()
{
    ui8 result = ParseSimple<ui8>();
    if (result > 1) {
        THROW_ERROR_EXCEPTION("Invalid boolean value %Qv", result);
    }
    return result;
}

TStringBuf TUncheckedSkiffParser::ParseString32()
{
    ui32 len = ParseSimple<ui32>();
    const void* data = GetData(len);
    return TStringBuf(static_cast<const char*>(data), len);
}

TStringBuf TUncheckedSkiffParser::ParseYson32()
{
    return ParseString32();
}

ui8 TUncheckedSkiffParser::ParseVariant8Tag()
{
    return ParseSimple<ui8>();
}

ui16 TUncheckedSkiffParser::ParseVariant16Tag()
{
    return ParseSimple<ui16>();
}

template <typename T>
T TUncheckedSkiffParser::ParseSimple()
{
    return ReadUnaligned<T>(GetData(sizeof(T)));
}

const void* TUncheckedSkiffParser::GetData(size_t size)
{
    if (RemainingBytes() >= size) {
        const void* result = Position_;
        Advance(size);
        return result;
    }

    return GetDataViaBuffer(size);
}

const void* TUncheckedSkiffParser::GetDataViaBuffer(size_t size)
{
    Buffer_.Clear();
    Buffer_.Reserve(size);
    while (Buffer_.Size() < size) {
        size_t toCopy = Min(size - Buffer_.Size(), RemainingBytes());
        Buffer_.Append(Position_, toCopy);
        Advance(toCopy);

        if (RemainingBytes() == 0) {
            RefillBuffer();
            if (Exhausted_ && Buffer_.Size() < size) {
                THROW_ERROR_EXCEPTION("Premature end of stream while parsing Skiff");
            }
        }
    }
    return Buffer_.Data();
}

size_t TUncheckedSkiffParser::RemainingBytes() const
{
    YT_ASSERT(End_ >= Position_);
    return End_ - Position_;
}

void TUncheckedSkiffParser::Advance(size_t size)
{
    YT_ASSERT(size <= RemainingBytes());
    Position_ += size;
    ReadBytesCount_ += size;
}

void TUncheckedSkiffParser::RefillBuffer()
{
    size_t bufferSize = Underlying_->Next(&Position_);
    End_ = Position_ + bufferSize;
    if (bufferSize == 0) {
        Exhausted_ = true;
    }
}

bool TUncheckedSkiffParser::HasMoreData()
{
    if (RemainingBytes() == 0 && !Exhausted_) {
        RefillBuffer();
    }
    return !(RemainingBytes() == 0 && Exhausted_);
}

void TUncheckedSkiffParser::ValidateFinished()
{ }

ui64 TUncheckedSkiffParser::GetReadBytesCount()
{
    return ReadBytesCount_;
}

////////////////////////////////////////////////////////////////////////////////

TCheckedSkiffParser::TCheckedSkiffParser(const TSkiffSchemaPtr& schema, IZeroCopyInput* stream)
    : Parser_(stream)
    , Validator_(std::make_unique<TSkiffValidator>(schema))
{ }

TCheckedSkiffParser::~TCheckedSkiffParser() = default;

i64 TCheckedSkiffParser::ParseInt64()
{
    Validator_->OnSimpleType(EWireType::Int64);
    return Parser_.ParseInt64();
}

ui64 TCheckedSkiffParser::ParseUint64()
{
    Validator_->OnSimpleType(EWireType::Uint64);
    return Parser_.ParseUint64();
}

double TCheckedSkiffParser::ParseDouble()
{
    Validator_->OnSimpleType(EWireType::Double);
    return Parser_.ParseDouble();
}

bool TCheckedSkiffParser::ParseBoolean()
{
    Validator_->OnSimpleType(EWireType::Boolean);
    return Parser_.ParseBoolean();
}

TStringBuf TCheckedSkiffParser::ParseString32()
{
    Validator_->OnSimpleType(EWireType::String32);
    return Parser_.ParseString32();
}

TStringBuf TCheckedSkiffParser::ParseYson32()
{
    Validator_->OnSimpleType(EWireType::Yson32);
    return Parser_.ParseYson32();
}

ui8 TCheckedSkiffParser::ParseVariant8Tag()
{
    Validator_->BeforeVariant8Tag();
    auto result = Parser_.ParseVariant8Tag();
    Validator_->OnVariant8Tag(result);
    return result;
}

ui16 TCheckedSkiffParser::ParseVariant16Tag()
{
    Validator_->BeforeVariant16Tag();
    auto result = Parser_.ParseVariant16Tag();
    Validator_->OnVariant16Tag(result);
    return result;
}

bool TCheckedSkiffParser::HasMoreData()
{
    return Parser_.HasMoreData();
}

void TCheckedSkiffParser::ValidateFinished()
{
    Validator_->ValidateFinished();
    Parser_.ValidateFinished();
}

ui64 TCheckedSkiffParser::GetReadBytesCount()
{
    return Parser_.GetReadBytesCount();
}

////////////////////////////////////////////////////////////////////////////////

TUncheckedSkiffWriter::TUncheckedSkiffWriter(IZeroCopyOutput* underlying)
    : Underlying_(underlying)
{ }

TUncheckedSkiffWriter::TUncheckedSkiffWriter(const TSkiffSchemaPtr& /*schema*/, IZeroCopyOutput* underlying)
    : TUncheckedSkiffWriter(underlying)
{ }

TUncheckedSkiffWriter::~TUncheckedSkiffWriter()
{
    try {
        Flush();
    } catch (...) {
    }
}

void TUncheckedSkiffWriter::WriteInt64(i64 value)
{
    WriteSimple<i64>(value);
}

void TUncheckedSkiffWriter::WriteUint64(ui64 value)
{
    WriteSimple<ui64>(value);
}

void TUncheckedSkiffWriter::WriteDouble(double value)
{
    return WriteSimple<double>(value);
}

void TUncheckedSkiffWriter::WriteBoolean(bool value)
{
    return WriteSimple<ui8>(value ? 1 : 0);
}

void TUncheckedSkiffWriter::WriteString32(TStringBuf value)
{
    WriteSimple<ui32>(value.size());
    Underlying_.Write(value.data(), value.size());
}

void TUncheckedSkiffWriter::WriteYson32(TStringBuf value)
{
    WriteSimple<ui32>(value.size());
    Underlying_.Write(value.data(), value.size());
}

void TUncheckedSkiffWriter::WriteVariant8Tag(ui8 tag)
{
    WriteSimple<ui8>(tag);
}

void TUncheckedSkiffWriter::WriteVariant16Tag(ui16 tag)
{
    WriteSimple<ui16>(tag);
}

void TUncheckedSkiffWriter::Flush()
{
    Underlying_.UndoRemaining();
}

template <typename T>
Y_FORCE_INLINE void TUncheckedSkiffWriter::WriteSimple(T value)
{
    Underlying_.Write(&value, sizeof(T));
}

void TUncheckedSkiffWriter::Finish()
{
    Flush();
}

////////////////////////////////////////////////////////////////////////////////

TCheckedSkiffWriter::TCheckedSkiffWriter(const TSkiffSchemaPtr& schema, IZeroCopyOutput* underlying)
    : Writer_(underlying)
    , Validator_(std::make_unique<TSkiffValidator>(schema))
{ }

TCheckedSkiffWriter::~TCheckedSkiffWriter() = default;

void TCheckedSkiffWriter::WriteDouble(double value)
{
    Validator_->OnSimpleType(EWireType::Double);
    Writer_.WriteDouble(value);
}

void TCheckedSkiffWriter::WriteBoolean(bool value)
{
    Validator_->OnSimpleType(EWireType::Boolean);
    Writer_.WriteBoolean(value);
}

void TCheckedSkiffWriter::WriteInt64(i64 value)
{
    Validator_->OnSimpleType(EWireType::Int64);
    Writer_.WriteInt64(value);
}

void TCheckedSkiffWriter::WriteString32(TStringBuf value)
{
    Validator_->OnSimpleType(EWireType::String32);
    Writer_.WriteString32(value);
}

void TCheckedSkiffWriter::WriteYson32(TStringBuf value)
{
    Validator_->OnSimpleType(EWireType::Yson32);
    Writer_.WriteYson32(value);
}

void TCheckedSkiffWriter::WriteVariant8Tag(ui8 tag)
{
    Validator_->OnVariant8Tag(tag);
    Writer_.WriteVariant8Tag(tag);
}

void TCheckedSkiffWriter::WriteVariant16Tag(ui16 tag)
{
    Validator_->OnVariant16Tag(tag);
    Writer_.WriteVariant16Tag(tag);
}

void TCheckedSkiffWriter::WriteUint64(ui64 value)
{
    Validator_->OnSimpleType(EWireType::Uint64);
    Writer_.WriteUint64(value);
}

void TCheckedSkiffWriter::Flush()
{
    Writer_.Flush();
}

void TCheckedSkiffWriter::Finish()
{
    Validator_->ValidateFinished();
    Writer_.Finish();
}

////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkiff
