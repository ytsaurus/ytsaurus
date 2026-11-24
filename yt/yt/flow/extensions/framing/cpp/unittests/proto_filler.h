#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/reflection.h>

#include <util/system/types.h>

namespace NYT::NFlow::NProtoPackerTest {

//////////////////////////////////////////////////////////////////////////////////

class TProtoFiller
{
public:
    void FillMessage(google::protobuf::Message& message, i32 depth = 3);

private:
    void HandleField(
        google::protobuf::Message& message,
        const google::protobuf::Reflection* reflection,
        const google::protobuf::FieldDescriptor* field,
        i32 depth);

    void HandleSingleField(
        google::protobuf::Message& message,
        const google::protobuf::Reflection* reflection,
        const google::protobuf::FieldDescriptor* field,
        i32 depth);

    void HandleRepeatedField(
        google::protobuf::Message& message,
        const google::protobuf::Reflection* reflection,
        const google::protobuf::FieldDescriptor* field,
        i32 depth);

    std::string RandomString(i32 maxLength = 10);

    const google::protobuf::EnumValueDescriptor* RandomEnum(const google::protobuf::FieldDescriptor* field);
};

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NProtoPackerTest
