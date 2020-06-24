#include <mapreduce/yt/tools/proto_migration/root_proto_lib/main.pb.h>

#include <mapreduce/yt/interface/protobuf_format.h>

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

    TProtobufFieldOptions defaultFieldOptions;
    ParseProtobufFieldOptions(descriptor->options().GetRepeatedExtension(default_field_flags), &defaultFieldOptions);

    for (int fieldIndex = 0; fieldIndex < descriptor->field_count(); ++fieldIndex) {
        auto* field = descriptor->field(fieldIndex);
        TProtobufFieldOptions fieldOptions = defaultFieldOptions;
        ParseProtobufFieldOptions(field->options().GetRepeatedExtension(flags), &fieldOptions);
        visitor(field, fieldOptions);
        if (field->type() == FieldDescriptor::TYPE_MESSAGE &&
            fieldOptions.SerializationMode == EProtobufSerializationMode::Yt)
        {
            Traverse(visited, field->message_type(), visitor);
        }
    }
}

bool AreFieldsOrderedByFieldNumber(const Descriptor* descriptor)
{
    for (int i = 0; i < descriptor->field_count() - 1; ++i) {
        if (descriptor->field(i)->number() > descriptor->field(i + 1)->number()) {
            return false;
        }
    }
    return true;
}

void Analyze(const Descriptor* descriptor)
{
    auto visitor = [] (const FieldDescriptor* field, const TProtobufFieldOptions& options) {
        if (field->type() == FieldDescriptor::TYPE_MESSAGE &&
            options.SerializationMode == EProtobufSerializationMode::Yt &&
            !AreFieldsOrderedByFieldNumber(field->message_type()))
        {
            Cout << field->message_type()->file()->name() << ": message " << field->message_type()->full_name() << Endl;
        }
    };
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

    Analyze(NYT::NProtoMigration::TRoot::descriptor());

    return 0;
}
