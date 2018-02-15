#include "checked_parser.h"

#include "skiff_validator.h"
#include "wire_type.h"

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

TCheckedSkiffParser::TCheckedSkiffParser(const TSkiffSchemaPtr& schema, IZeroCopyInput* stream)
    : Parser_(stream)
    , Validator_(::MakeHolder<TSkiffValidator>(schema))
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

////////////////////////////////////////////////////////////////////

} // namespace NSkiff
