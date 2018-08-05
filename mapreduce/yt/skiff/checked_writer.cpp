#include "checked_writer.h"

#include "skiff_validator.h"
#include "wire_type.h"

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

TCheckedSkiffWriter::TCheckedSkiffWriter(const TSkiffSchemaPtr& schema, IOutputStream* underlying)
    : Writer_(underlying)
    , Validator_(::MakeHolder<TSkiffValidator>(schema))
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


void TCheckedSkiffWriter::SetBufferCapacity(size_t s) {
    Writer_.SetBufferCapacity(s);
}

////////////////////////////////////////////////////////////////////

} // namespace NSkiff
