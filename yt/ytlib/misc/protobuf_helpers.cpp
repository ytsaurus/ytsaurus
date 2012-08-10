#include "stdafx.h"
#include "protobuf_helpers.h"

#include <contrib/libs/protobuf/io/zero_copy_stream.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool SerializeToProto(const google::protobuf::Message* message, TSharedRef* data)
{
    int size = message->ByteSize();
    *data = TSharedRef(size);
    return message->SerializeToArray(data->Begin(), size);
}

bool DeserializeFromProto(google::protobuf::Message* message, TRef data)
{
    return message->ParseFromArray(data.Begin(), data.Size());
}

////////////////////////////////////////////////////////////////////////////////

void SaveProto(TOutputStream* output, const ::google::protobuf::Message& message)
{
    TSharedRef ref;
    YCHECK(SerializeToProto(&message, &ref));
    ::SaveSize(output, ref.Size());
    output->Write(ref.Begin(), ref.Size());
}

void LoadProto(TInputStream* input, ::google::protobuf::Message& message)
{
    size_t size = ::LoadSize(input);
    TSharedRef ref(size);
    YCHECK(input->Load(ref.Begin(), size) == size);
    YCHECK(DeserializeFromProto(&message, ref));
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

