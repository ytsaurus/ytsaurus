#include "skiff.h"

#include "skiff_validator.h"

#include <yt/core/misc/error.h>

namespace NYT::NSkiff {

////////////////////////////////////////////////////////////////////////////////

TUncheckedSkiffParser::TUncheckedSkiffParser(IInputStream* underlying)
    : Underlying_(underlying)
    , Buffer_(512_KB)
    , RemainingBytes_(0)
    , Position_(Buffer_.Data())
{ }

TUncheckedSkiffParser::TUncheckedSkiffParser(const TSkiffSchemaPtr& /*schema*/, IInputStream* underlying)
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
        THROW_ERROR_EXCEPTION("Invalid boolean value %Qv",
            result);
    }
    return result;
}

const TString& TUncheckedSkiffParser::ParseString32(TString* result)
{
    ui32 toRead = ParseSimple<ui32>();
    result->clear();
    result->reserve(toRead);

    while (true) {
        ui32 curRead = Min<ui32>(toRead, RemainingBytes_);
        result->append(Position_, curRead);
        toRead -= curRead;
        Advance(curRead);
        if (toRead == 0) {
            break;
        }
        RefillBuffer(1);
    }
    return *result;
}

TString TUncheckedSkiffParser::ParseString32()
{
    TString result;
    ParseString32(&result);
    return result;
}

const TString& TUncheckedSkiffParser::ParseYson32(TString* result)
{
    return ParseString32(result);
}

TString TUncheckedSkiffParser::ParseYson32()
{
    TString result;
    ParseYson32(&result);
    return result;
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
    if (RemainingBytes_ < sizeof(T)) {
        RefillBuffer(sizeof(T));
    }
    T result = *reinterpret_cast<T*>(Position_);
    Advance(sizeof(T));
    return result;
}

void TUncheckedSkiffParser::RefillBuffer(size_t minSize)
{
    memmove(Buffer_.Data(), Position_, RemainingBytes_);
    Position_ = Buffer_.Data();
    do {
        size_t toRead = Buffer_.Capacity() - RemainingBytes_;
        size_t read = Underlying_->Read(Position_ + RemainingBytes_, toRead);
        if (read == 0) {
            Exhausted_ = true;
            if (RemainingBytes_ < minSize) {
                THROW_ERROR_EXCEPTION("Premature end of stream while parsing Skiff");
            }
        }
        RemainingBytes_ += read;
    } while (RemainingBytes_ < minSize);
}

void TUncheckedSkiffParser::Advance(ssize_t size)
{
    Y_ASSERT(size <= (ssize_t)RemainingBytes_);
    Position_ += size;
    RemainingBytes_ -= size;
    ReadBytesCount_ += size;
}

bool TUncheckedSkiffParser::HasMoreData()
{
    if (RemainingBytes_ > 0) {
        return true;
    }
    if (Exhausted_) {
        return false;
    }
    RefillBuffer(0);
    return RemainingBytes_ > 0;
}

void TUncheckedSkiffParser::ValidateFinished()
{ }

ui64 TUncheckedSkiffParser::GetReadBytesCount()
{
    return ReadBytesCount_;
}

////////////////////////////////////////////////////////////////////////////////

TCheckedSkiffParser::TCheckedSkiffParser(const TSkiffSchemaPtr& schema, IInputStream* stream)
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

const TString& TCheckedSkiffParser::ParseString32(TString* result)
{
    Validator_->OnSimpleType(EWireType::String32);
    return Parser_.ParseString32(result);
}

TString TCheckedSkiffParser::ParseString32()
{
    Validator_->OnSimpleType(EWireType::String32);
    return Parser_.ParseString32();
}

const TString& TCheckedSkiffParser::ParseYson32(TString* result)
{
    Validator_->OnSimpleType(EWireType::Yson32);
    return Parser_.ParseYson32(result);
}

TString TCheckedSkiffParser::ParseYson32()
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

TUncheckedSkiffWriter::TUncheckedSkiffWriter(IOutputStream* underlying)
    : Underlying_(underlying)
    , Buffer_(1_KB)
    , RemainingBytes_(Buffer_.Capacity())
    , Position_(Buffer_.Data())
{ }

TUncheckedSkiffWriter::TUncheckedSkiffWriter(const TSkiffSchemaPtr& /*schema*/, IOutputStream* underlying)
    : TUncheckedSkiffWriter(underlying)
{ }

TUncheckedSkiffWriter::~TUncheckedSkiffWriter()
{
    try {
        DoFlush();
    } catch (...) {
    }
}

template <typename T>
void TUncheckedSkiffWriter::WriteSimple(T value)
{
    if (RemainingBytes_ < sizeof(T)) {
        DoFlush();
        Y_ASSERT(RemainingBytes_ >= sizeof(T));
    }
    *reinterpret_cast<T*>(Position_) = value;
    Position_ += sizeof(T);
    RemainingBytes_ -= sizeof(T);
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
    WriteSimple<ui32>(value.Size());
    TUncheckedSkiffWriter::DoWrite(value.Data(), value.Size());
}

void TUncheckedSkiffWriter::WriteYson32(TStringBuf value)
{
    WriteSimple<ui32>(value.Size());
    TUncheckedSkiffWriter::DoWrite(value.Data(), value.Size());
}

void TUncheckedSkiffWriter::WriteVariant8Tag(ui8 tag)
{
    WriteSimple<ui8>(tag);
}

void TUncheckedSkiffWriter::WriteVariant16Tag(ui16 tag)
{
    WriteSimple<ui16>(tag);
}

void TUncheckedSkiffWriter::DoFlush()
{
    Underlying_->Write(Buffer_.Data(), Position_ - Buffer_.Data());
    Position_ = Buffer_.Data();
    RemainingBytes_ = Buffer_.Capacity();
}

void TUncheckedSkiffWriter::DoWrite(const void* data, size_t size)
{
    if (size > RemainingBytes_) {
        DoFlush();
        if (size >= RemainingBytes_) {
            Underlying_->Write(data, size);
            return;
        }
    }
    memcpy(Position_, data, size);
    Position_ += size;
    RemainingBytes_ -= size;
}

void TUncheckedSkiffWriter::Finish()
{
    IOutputStream::Flush();
}

////////////////////////////////////////////////////////////////////////////////

TCheckedSkiffWriter::TCheckedSkiffWriter(const TSkiffSchemaPtr& schema, IOutputStream* underlying)
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
