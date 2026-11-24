#include "proto_filler.h"

#include <library/cpp/yt/error/error.h>
#include <util/random/random.h>

namespace NYT::NFlow::NProtoPackerTest {

//////////////////////////////////////////////////////////////////////////////////

using namespace google::protobuf;

void TProtoFiller::FillMessage(Message& message, i32 depth)
{
    if (depth <= 0) {
        return;
    }

    const Descriptor* descriptor = message.GetDescriptor();
    const Reflection* reflection = message.GetReflection();

    for (i32 i = 0; i < descriptor->field_count(); ++i) {
        const FieldDescriptor* field = descriptor->field(i);
        if (field->is_repeated()) {
            HandleRepeatedField(message, reflection, field, depth);
        } else if (field->is_optional()) {
            HandleSingleField(message, reflection, field, depth);
        } else {
            THROW_ERROR_EXCEPTION("Unsupported field type");
        }
    }
}

void TProtoFiller::HandleSingleField(
    Message& message,
    const Reflection* reflection,
    const FieldDescriptor* field,
    i32 depth)
{
    if (reflection->HasField(message, field)) {
        return;
    }

    switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            reflection->SetInt32(&message, field, RandomNumber<ui32>());
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            reflection->SetInt64(&message, field, RandomNumber<ui64>());
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            reflection->SetUInt32(&message, field, RandomNumber<ui32>());
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            reflection->SetUInt64(&message, field, RandomNumber<ui64>());
            break;
        case FieldDescriptor::CPPTYPE_DOUBLE:
            reflection->SetDouble(&message, field, RandomNumber<double>());
            break;
        case FieldDescriptor::CPPTYPE_FLOAT:
            reflection->SetFloat(&message, field, RandomNumber<float>());
            break;
        case FieldDescriptor::CPPTYPE_BOOL:
            reflection->SetBool(&message, field, RandomNumber<bool>());
            break;
        case FieldDescriptor::CPPTYPE_STRING:
            reflection->SetString(&message, field, RandomString());
            break;
        case FieldDescriptor::CPPTYPE_ENUM:
            reflection->SetEnum(&message, field, RandomEnum(field));
            break;
        case FieldDescriptor::CPPTYPE_MESSAGE:
            FillMessage(*reflection->MutableMessage(&message, field), depth - 1);
            break;
        default:
            break;
    }
}

void TProtoFiller::HandleRepeatedField(
    Message& message,
    const Reflection* reflection,
    const FieldDescriptor* field,
    i32 depth)
{
    const i32 count = RandomNumber<ui32>(3) + 1;

    for (i32 i = 0; i < count; ++i) {
        switch (field->cpp_type()) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflection->AddInt32(&message, field, RandomNumber<ui32>());
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                reflection->AddInt64(&message, field, RandomNumber<ui64>());
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflection->AddUInt32(&message, field, RandomNumber<ui32>());
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflection->AddUInt64(&message, field, RandomNumber<ui64>());
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflection->AddDouble(&message, field, RandomNumber<double>());
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflection->AddFloat(&message, field, RandomNumber<float>());
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflection->AddBool(&message, field, RandomNumber<bool>());
                break;
            case FieldDescriptor::CPPTYPE_STRING:
                reflection->AddString(&message, field, RandomString());
                break;
            case FieldDescriptor::CPPTYPE_ENUM:
                reflection->AddEnum(&message, field, RandomEnum(field));
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE: {
                Message* subMsg = reflection->AddMessage(&message, field);
                FillMessage(*subMsg, depth - 1);
                break;
            }
            default:
                break;
        }
    }
}

std::string TProtoFiller::RandomString(i32 maxLength)
{
    std::string str(RandomNumber<ui32>(maxLength), ' ');
    for (auto& c : str) {
        c = static_cast<char>('a' + RandomNumber<ui32>(27));
    }
    return str;
}

const EnumValueDescriptor* TProtoFiller::RandomEnum(const FieldDescriptor* field)
{
    const EnumDescriptor* desc = field->enum_type();
    return desc->value(RandomNumber<ui32>(desc->value_count()));
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NProtoPackerTest
