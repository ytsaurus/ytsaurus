#include "stdafx.h"
#include "protobuf_helpers.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const size_t MinOutputBlobSize = 256;

class TBlobOutputStream
    : public google::protobuf::io::ZeroCopyOutputStream
{
public:
    explicit TBlobOutputStream(TBlob* blob)
        : Blob(blob)
    {
        Blob->clear();
    }

    virtual bool Next(void** data, int* size) override
    {
        size_t oldSize = Blob->size();

        if (oldSize < Blob->capacity()) {
            Blob->resize(Blob->capacity());
        } else {
            Blob->resize(std::max(oldSize * 2, MinOutputBlobSize));
        }

        *data = Blob->data() + oldSize;
        *size = Blob->size() - oldSize;
        return true;
    }

    void BackUp(int count) override
    {
        Blob->resize(Blob->size() - count);
    }

    i64 ByteCount() const override
    {
        return Blob->size();
    }

private:
    TBlob* Blob;

};

bool SerializeToProto(const google::protobuf::Message* message, TBlob* data)
{
    TBlobOutputStream zeroCopyStream(data);
    google::protobuf::io::CodedOutputStream codedOutputStream(&zeroCopyStream);
    return message->SerializePartialToCodedStream(&codedOutputStream);
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

