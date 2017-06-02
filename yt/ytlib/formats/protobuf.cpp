#include "protobuf.h"

namespace NYT {
namespace NFormats {

using ::google::protobuf::Descriptor;
using ::google::protobuf::FileDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::DescriptorPool;

////////////////////////////////////////////////////////////////////////////////

class TProtoErrorCollector
    : public DescriptorPool::ErrorCollector
{
public:
    virtual void AddError(
        const TString& fileName,
        const TString& elementName,
        const Message* descriptor,
        DescriptorPool::ErrorCollector::ErrorLocation location,
        const TString& message) override
    {
        THROW_ERROR_EXCEPTION("Error while building protobuf descriptors: %v", message)
            << TErrorAttribute("file_name", fileName)
            << TErrorAttribute("element_name", elementName);
    }
};

////////////////////////////////////////////////////////////////////////////////

TProtobufTypeSet::TProtobufTypeSet(TProtobufFormatConfigPtr config)
{
    TStringInput input(config->FileDescriptorSet);
    if (!FileDescriptorSet_.ParseFromStream(&input)) {
        THROW_ERROR_EXCEPTION("Error parsing \"file_descriptor_set\" in protobuf config");
    }

    std::vector<const FileDescriptor*> fileDescriptors;

    TProtoErrorCollector errorCollector;

    for (const auto& fileDescriptorProto : FileDescriptorSet_.file()) {
        auto* file = DescriptorPool_.BuildFileCollectingErrors(
            fileDescriptorProto,
            &errorCollector);

        if (!file) {
            THROW_ERROR_EXCEPTION("Error building file %v",
                fileDescriptorProto.name());
        }

        fileDescriptors.push_back(file);
    }

    for (size_t i = 0; i < config->FileIndices.size(); ++i) {
        auto* fileDescriptor = fileDescriptors[config->FileIndices[i]];
        MessageDescriptors_.push_back(
            fileDescriptor->message_type(config->MessageIndices[i]));
    }
}

const Descriptor* TProtobufTypeSet::GetMessageDescriptor(int tableIndex) const
{
    if (tableIndex >= MessageDescriptors_.size()) {
        THROW_ERROR_EXCEPTION("Invalid table index %v, only %v descriptors are available",
            tableIndex,
            MessageDescriptors_.size());
    }
    return MessageDescriptors_[tableIndex];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

