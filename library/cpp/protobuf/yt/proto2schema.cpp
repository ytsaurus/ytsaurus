#include "proto2schema.h"

#include "column_name.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

static bool CheckGoogleTimestamp(const ::google::protobuf::Message& proto, const ::google::protobuf::FieldDescriptor& field) {
    if (!field.is_repeated()) {
        auto r = proto.GetReflection();
        const auto& msg = r->GetMessage(proto, &field);
        return (msg.GetTypeName() == TParseConfig::GOOGLE_PROTOBUF_TIMESTAMP_TYPE);
    }
    return false;
}

static NYT::EValueType GetColumnType(const ::google::protobuf::Message& proto,
                                    const ::google::protobuf::FieldDescriptor& field,
                                    const TParseConfig config) {
    using namespace ::google::protobuf;
    if (field.is_repeated()) {
        return NYT::EValueType::VT_ANY;
    }
    switch (field.cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
        case FieldDescriptor::CPPTYPE_INT64:
            return NYT::EValueType::VT_INT64;
        case FieldDescriptor::CPPTYPE_UINT32:
        case FieldDescriptor::CPPTYPE_UINT64:
            return NYT::EValueType::VT_UINT64;
        case FieldDescriptor::CPPTYPE_FLOAT:
        case FieldDescriptor::CPPTYPE_DOUBLE:
            return NYT::EValueType::VT_DOUBLE;
        case FieldDescriptor::CPPTYPE_BOOL:
            return NYT::EValueType::VT_BOOLEAN;
        case FieldDescriptor::CPPTYPE_STRING:
            return NYT::EValueType::VT_STRING;
        case FieldDescriptor::CPPTYPE_MESSAGE:
            if (config.GoogleTimestampToInt64 && CheckGoogleTimestamp(proto, field)) {
                return NYT::EValueType::VT_INT64;
            }
            [[fallthrough]];
        case FieldDescriptor::CPPTYPE_ENUM:
            return NYT::EValueType::VT_ANY;
        default:
            ythrow yexception() << "Unexpected field type '" << field.cpp_type_name() << "' for field " << field.name();
    }
}

static bool HasExtension(const ::google::protobuf::FieldDescriptor& field) {
    const auto& o = field.options();
    return o.HasExtension(NYT::column_name) || o.HasExtension(NYT::key_column_name);
}

NYT::TTableSchema CreateTableSchemaFromProto(const ::google::protobuf::Message& proto, const TParseConfig& config) {
    NYT::TTableSchema result;
    auto keyIt = config.KeyColumns.Parts_.begin();
    const auto keyLim = config.KeyColumns.Parts_.end();
    auto d = proto.GetDescriptor();
    for (int idx = 0, lim = d->field_count(); idx < lim; ++idx) {
        const auto field = d->field(idx);
        if (!config.KeepFieldsWithoutExtension && !HasExtension(*field)) {
            continue;
        }

        const auto name = CalcColumnName(*field, config.UseYtExtention, config.UseToLower);
        NYT::TColumnSchema column;
        column.Name(name);
        column.Type(GetColumnType(proto, /*required*/ *field, config), field->is_required());
        if (keyIt != keyLim) {
            Y_ENSURE(*keyIt == name, "Key columns list should be prefix of schema");
            column.SortOrder(NYT::SO_ASCENDING);
            ++keyIt;
        }
        result.AddColumn(column);
    }
    return result;
}
