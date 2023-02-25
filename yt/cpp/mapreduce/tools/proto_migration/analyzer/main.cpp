#include <yt/cpp/mapreduce/tools/proto_migration/root_proto_lib/main.pb.h>

#include <yt/cpp/mapreduce/interface/protobuf_format.h>

#include <library/cpp/getopt/last_getopt.h>

using namespace NYT;
using namespace NYT::NDetail;

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;

using TVisitor = std::function<void(const FieldDescriptor*, const TProtobufFieldOptions&)>;

void Traverse(
    THashSet<const Descriptor*>& visited,
    const Descriptor* descriptor,
    TVisitor visitor)
{
    if (visited.contains(descriptor)) {
        return;
    }
    visited.insert(descriptor);

    Cerr << "Visiting " << descriptor->full_name() << Endl;

    for (int fieldIndex = 0; fieldIndex < descriptor->field_count(); ++fieldIndex) {
        auto* field = descriptor->field(fieldIndex);
        auto fieldOptions = GetFieldOptions(field);
        visitor(field, fieldOptions);
        if (field->type() == FieldDescriptor::TYPE_MESSAGE &&
            fieldOptions.SerializationMode == EProtobufSerializationMode::Yt)
        {
            Traverse(visited, field->message_type(), visitor);
        }
    }
}

void PrintUnorderedMessages(const FieldDescriptor* field, const TProtobufFieldOptions& options)
{
    auto areFieldsOrderedByFieldNumber = [](const Descriptor* descriptor) {
        for (int i = 0; i < descriptor->field_count() - 1; ++i) {
            if (descriptor->field(i)->number() > descriptor->field(i + 1)->number()) {
                return false;
            }
        }
        return true;
    };
    if (field->type() == FieldDescriptor::TYPE_MESSAGE &&
        options.SerializationMode == EProtobufSerializationMode::Yt &&
        !areFieldsOrderedByFieldNumber(field->message_type()))
    {
        Cout << field->message_type()->file()->name() << ": message " << field->message_type()->full_name() << Endl;
    }
}

void PrintMaps(const FieldDescriptor* field, const TProtobufFieldOptions& options)
{
    if (options.SerializationMode == EProtobufSerializationMode::Yt && field->is_map()) {
        Cout << field->file()->name() << ": message " << field->full_name() << ", field " << field->name() << " = " << field->index() << Endl;
    }
}

void PrintVariants(const FieldDescriptor* field, const TProtobufFieldOptions& options)
{
    Y_UNUSED(options);
    if (auto oneof = field->containing_oneof()) {
        auto opt = GetOneofOptions(oneof);
        if (opt.Mode == EProtobufOneofMode::Variant) {
            Cout << field->file()->name() << ": message " << field->full_name() << ", field " << field->name() << " = " << field->index() << Endl;
        }
    }
}

void Analyze(const Descriptor* descriptor, TVisitor visitor)
{
    THashSet<const Descriptor*> visited;
    for (int fieldIndex = 0; fieldIndex < descriptor->field_count(); ++fieldIndex) {
        auto* field = descriptor->field(fieldIndex);
        Traverse(visited, field->message_type(), visitor);
    }
}

int main(int argc, char** argv)
{
    auto opts = NLastGetopt::TOpts::Default();

    opts.SetTitle("Analyze protobuf messages");

    NLastGetopt::TOptsParseResult args(&opts, argc, argv);

    Analyze(NYT::NProtoMigration::TRoot::descriptor(), PrintVariants);

    return 0;
}
