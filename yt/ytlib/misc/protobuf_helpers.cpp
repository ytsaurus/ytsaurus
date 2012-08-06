#include "stdafx.h"
#include "protobuf_helpers.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool SerializeToProto(const google::protobuf::Message* message, TBlob* data)
{
    int size = message->ByteSize();
    data->resize(size);
    return message->SerializeToArray(&*data->begin(), size);
}

bool DeserializeFromProto(google::protobuf::Message* message, TRef data)
{
    return message->ParseFromArray(&*data.Begin(), data.Size());
}

////////////////////////////////////////////////////////////////////////////////

void SaveProto(TOutputStream* output, const ::google::protobuf::Message& message)
{
    TBlob blob;
    YCHECK(SerializeToProto(&message, &blob));
    ::SaveSize(output, blob.size());
    output->Write(&*blob.begin(), blob.size());
}

void LoadProto(TInputStream* input, ::google::protobuf::Message& message)
{
    size_t size = ::LoadSize(input);
    TBlob blob(size);
    YCHECK(input->Load(&*blob.begin(), size) == size);
    YCHECK(DeserializeFromProto(&message, TRef::FromBlob(blob)));
}

void FilterProtoExtensions(
    NProto::TExtensionSet* target,
    const NProto::TExtensionSet& source,
    const yhash_set<int>& tags)
{
    target->Clear();
    FOREACH (const auto& extension, source.extensions()) {
        if (tags.find(extension.tag()) != tags.end()) {
            *target->add_extensions() = extension;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

