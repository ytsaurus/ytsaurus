#include "format.h"

#include "errors.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <contrib/libs/protobuf/descriptor.h>
#include <contrib/libs/protobuf/google/protobuf/descriptor.pb.h>
#include <contrib/libs/protobuf/messagext.h>

#include <util/generic/hash_set.h>

#include <util/stream/output.h>

namespace NYT {

using ::google::protobuf::Message;
using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::FileDescriptor;
using ::google::protobuf::FileDescriptorSet;

namespace {

////////////////////////////////////////////////////////////////////////////////

using NDetail::EProtobufType;

TString GetColumnName(const FieldDescriptor& field) {
    const auto& options = field.options();
    const auto columnName = options.GetExtension(column_name);
    if (!columnName.empty()) {
        return columnName;
    }
    const auto keyColumnName = options.GetExtension(key_column_name);
    if (!keyColumnName.empty()) {
        return keyColumnName;
    }
    return field.name();
}

enum class EProtobufSerializationMode
{
    Protobuf,
    Yt,
};

enum class EListMode
{
    Optional,
    Required,
};

struct TProtobufFieldOptions
{
    TMaybe<EProtobufType> Type;
    EProtobufSerializationMode SerializationMode = EProtobufSerializationMode::Protobuf;
    EListMode ListMode = EListMode::Required;
};

using TOption = TVariant<EProtobufType, EProtobufSerializationMode, EListMode>;

TOption FieldFlagToOption(EWrapperFieldFlag::Enum flag)
{
    using EFlag = EWrapperFieldFlag;
    switch (flag) {
        case EFlag::SERIALIZATION_PROTOBUF:
            return EProtobufSerializationMode::Protobuf;
        case EFlag::SERIALIZATION_YT:
            return EProtobufSerializationMode::Yt;
        case EFlag::ANY:
            return EProtobufType::Any;
        case EFlag::OTHER_COLUMNS:
            return EProtobufType::OtherColumns;
        case EFlag::ENUM_INT:
            return EProtobufType::EnumInt;
        case EFlag::ENUM_STRING:
            return EProtobufType::EnumString;
        case EFlag::OPTIONAL_LIST:
            return EListMode::Optional;
        case EFlag::REQUIRED_LIST:
            return EListMode::Required;
    }
    Y_FAIL();
}

EWrapperFieldFlag::Enum OptionToFieldFlag(TOption option)
{
    using EFlag = EWrapperFieldFlag;
    struct TVisitor
    {
        EFlag::Enum operator() (EProtobufType type)
        {
            switch (type) {
                case EProtobufType::Any:
                    return EFlag::ANY;
                case EProtobufType::OtherColumns:
                    return EFlag::OTHER_COLUMNS;
                case EProtobufType::EnumInt:
                    return EFlag::ENUM_INT;
                case EProtobufType::EnumString:
                    return EFlag::ENUM_STRING;
            }
            Y_FAIL();
        }
        EFlag::Enum operator() (EProtobufSerializationMode serializationMode)
        {
            switch (serializationMode) {
                case EProtobufSerializationMode::Yt:
                    return EFlag::SERIALIZATION_YT;
                case EProtobufSerializationMode::Protobuf:
                    return EFlag::SERIALIZATION_PROTOBUF;
            }
            Y_FAIL();
        }
        EFlag::Enum operator() (EListMode listMode)
        {
            switch (listMode) {
                case EListMode::Optional:
                    return EFlag::OPTIONAL_LIST;
                case EListMode::Required:
                    return EFlag::REQUIRED_LIST;
            }
            Y_FAIL();
        }
    };

    return Visit(TVisitor(), option);
}

class TParseProtobufFieldOptionsVisitor
{
public:
    void operator() (EProtobufType type)
    {
        SetOption(Type, type);
    }

    void operator() (EProtobufSerializationMode serializationMode)
    {
        SetOption(SerializationMode, serializationMode);
    }

    void operator() (EListMode listMode)
    {
        SetOption(ListMode, listMode);
    }

    template <typename T>
    void SetOption(TMaybe<T>& option, T newOption) {
        if (option) {
            if (*option == newOption) {
                ythrow yexception() << "Duplicate protobuf field flag " << OptionToFieldFlag(newOption);
            } else {
                ythrow yexception() << "Incompatible protobuf field flags " <<
                    OptionToFieldFlag(*option) << " and " << OptionToFieldFlag(newOption);
            }
        }
        option = newOption;
    };

public:
    TMaybe<EProtobufType> Type;
    TMaybe<EProtobufSerializationMode> SerializationMode;
    TMaybe<EListMode> ListMode;
};

void ParseProtobufFieldOptions(
    const ::google::protobuf::RepeatedField<EWrapperFieldFlag::Enum>& flags,
    TProtobufFieldOptions* fieldOptions)
{
    TParseProtobufFieldOptionsVisitor visitor;
    for (auto flag : flags) {
        Visit(visitor, FieldFlagToOption(flag));
    }
    if (visitor.Type) {
        fieldOptions->Type = *visitor.Type;
    }
    if (visitor.SerializationMode) {
        fieldOptions->SerializationMode = *visitor.SerializationMode;
    }
    if (visitor.ListMode) {
        fieldOptions->ListMode = *visitor.ListMode;
    }
}

void ValidateProtobufType(const FieldDescriptor& fieldDescriptor, EProtobufType protobufType)
{
    const auto fieldType = fieldDescriptor.type();
    auto ensureType = [&] (FieldDescriptor::Type expectedType) {
        Y_ENSURE(fieldType == expectedType,
            "Type of field " << fieldDescriptor.name() << "does not match specified field flag " << OptionToFieldFlag(protobufType) << ": "
            "expected " << FieldDescriptor::TypeName(expectedType) << ", got " << FieldDescriptor::TypeName(fieldType));
    };
    switch (protobufType) {
        case EProtobufType::Any:
            ensureType(FieldDescriptor::TYPE_BYTES);
            return;
        case EProtobufType::OtherColumns:
            ensureType(FieldDescriptor::TYPE_BYTES);
            return;
        case EProtobufType::EnumInt:
            ensureType(FieldDescriptor::TYPE_ENUM);
            return;
        case EProtobufType::EnumString:
            ensureType(FieldDescriptor::TYPE_ENUM);
            return;
    }
    Y_FAIL();
}

////////////////////////////////////////////////////////////////////////////////

TNode MakeEnumerationConfig(const ::google::protobuf::EnumDescriptor* enumDescriptor)
{
    auto config = TNode::CreateMap();
    for (int i = 0; i < enumDescriptor->value_count(); ++i) {
        config[enumDescriptor->value(i)->name()] = enumDescriptor->value(i)->number();
    }
    return config;
}

TString DeduceProtobufType(
    const FieldDescriptor* fieldDescriptor,
    const TProtobufFieldOptions& options)
{
    if (options.Type) {
        ValidateProtobufType(*fieldDescriptor, *options.Type);
        return ::ToString(*options.Type);
    }
    switch (fieldDescriptor->type()) {
        case FieldDescriptor::TYPE_ENUM:
            return ::ToString(EProtobufType::EnumString);
        case FieldDescriptor::TYPE_MESSAGE:
            switch (options.SerializationMode) {
                case EProtobufSerializationMode::Protobuf:
                    return fieldDescriptor->type_name();
                case EProtobufSerializationMode::Yt:
                    return "structured_message";
            }
            Y_FAIL();
        default:
            return fieldDescriptor->type_name();
    }
    Y_FAIL();
}

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations);

TNode MakeProtoFormatFieldConfig(
    const FieldDescriptor* fieldDescriptor,
    TNode* enumerations,
    const TProtobufFieldOptions& defaultOptions)
{
    auto fieldConfig = TNode::CreateMap();
    fieldConfig["field_number"] = fieldDescriptor->number();
    fieldConfig["name"] = GetColumnName(*fieldDescriptor);

    const auto& options = fieldDescriptor->options();

    auto fieldOptions = defaultOptions;
    ParseProtobufFieldOptions(options.GetRepeatedExtension(flags), &fieldOptions);

    if (fieldDescriptor->is_repeated()) {
        Y_ENSURE_EX(fieldOptions.SerializationMode == EProtobufSerializationMode::Yt,
            TApiUsageError() << "Repeated field " << fieldDescriptor->full_name() << ' ' <<
            "must have flag " << EWrapperFieldFlag::SERIALIZATION_YT);
    }
    fieldConfig["repeated"] = fieldDescriptor->is_repeated();

    fieldConfig["proto_type"] = DeduceProtobufType(fieldDescriptor, fieldOptions);

    if (fieldDescriptor->type() == FieldDescriptor::TYPE_ENUM) {
        auto* enumeration = fieldDescriptor->enum_type();
        (*enumerations)[enumeration->name()] = MakeEnumerationConfig(enumeration);
        fieldConfig["enumeration_name"] = enumeration->name();
    } else if (
        fieldDescriptor->type() == FieldDescriptor::TYPE_MESSAGE &&
        fieldOptions.SerializationMode == EProtobufSerializationMode::Yt)
    {
        fieldConfig["fields"] = MakeProtoFormatMessageFieldsConfig(fieldDescriptor->message_type(), enumerations);
    }

    return fieldConfig;
}

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations)
{
    TNode fields = TNode::CreateList();
    TProtobufFieldOptions fieldOptions;
    ParseProtobufFieldOptions(descriptor->options().GetRepeatedExtension(default_field_flags), &fieldOptions);
    for (int fieldIndex = 0; fieldIndex < descriptor->field_count(); ++fieldIndex) {
        auto* fieldDesc = descriptor->field(fieldIndex);
        fields.Add(MakeProtoFormatFieldConfig(
            fieldDesc,
            enumerations,
            fieldOptions));
    }
    return fields;
}

TNode MakeProtoFormatConfig(const TVector<const Descriptor*>& descriptors)
{
    TNode config("protobuf");
    config.Attributes()
        ("enumerations", TNode::CreateMap())
        ("tables", TNode::CreateList());

    auto& enumerations = config.Attributes()["enumerations"];

    for (auto* descriptor : descriptors) {
        auto columns = MakeProtoFormatMessageFieldsConfig(descriptor, &enumerations);
        config.Attributes()["tables"].Add(
            TNode()("columns", std::move(columns)));
    }

    return config;
}

////////////////////////////////////////////////////////////////////////////////

struct TOtherColumns
{ };

static TVariant<EValueType, TOtherColumns> GetScalarFieldType(
    const FieldDescriptor& fieldDescriptor,
    const TProtobufFieldOptions& options)
{
    if (options.Type) {
        switch (*options.Type) {
            case EProtobufType::EnumInt:
                return EValueType::VT_INT64;
            case EProtobufType::EnumString:
                return EValueType::VT_STRING;
            case EProtobufType::Any:
                return EValueType::VT_ANY;
            case EProtobufType::OtherColumns:
                return TOtherColumns{};
        }
        Y_FAIL();
    }

    switch (fieldDescriptor.cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
            return EValueType::VT_INT32;
        case FieldDescriptor::CPPTYPE_INT64:
            return EValueType::VT_INT64;
        case FieldDescriptor::CPPTYPE_UINT32:
            return EValueType::VT_UINT32;
        case FieldDescriptor::CPPTYPE_UINT64:
            return EValueType::VT_UINT64;
        case FieldDescriptor::CPPTYPE_FLOAT:
        case FieldDescriptor::CPPTYPE_DOUBLE:
            return EValueType::VT_DOUBLE;
        case FieldDescriptor::CPPTYPE_BOOL:
            return EValueType::VT_BOOLEAN;
        case FieldDescriptor::CPPTYPE_STRING:
        case FieldDescriptor::CPPTYPE_MESSAGE:
        case FieldDescriptor::CPPTYPE_ENUM:
            return EValueType::VT_STRING;
        default:
            ythrow yexception() <<
                "Unexpected field type '" << fieldDescriptor.cpp_type_name() << "' " <<
                "for field " << fieldDescriptor.name();
    }
}

bool HasNameExtension(const FieldDescriptor& fieldDescriptor)
{
    const auto& options = fieldDescriptor.options();
    return options.HasExtension(column_name) || options.HasExtension(key_column_name);
}

TVariant<NTi::TTypePtr, TOtherColumns> GetFieldType(
    const FieldDescriptor& fieldDescriptor,
    const TProtobufFieldOptions& defaultOptions)
{
    auto options = defaultOptions;
    ParseProtobufFieldOptions(
        fieldDescriptor.options().GetRepeatedExtension(flags),
        &options);

    if (options.Type) {
        ValidateProtobufType(fieldDescriptor, *options.Type);
    }

    NTi::TTypePtr type;
    if (fieldDescriptor.type() == FieldDescriptor::TYPE_MESSAGE &&
        options.SerializationMode == EProtobufSerializationMode::Yt)
    {
        const auto& messageDescriptor = *fieldDescriptor.message_type();
        TProtobufFieldOptions embeddedMessageDefaultOptions;
        ParseProtobufFieldOptions(
            messageDescriptor.options().GetRepeatedExtension(default_field_flags),
            &embeddedMessageDefaultOptions);

        TVector<NTi::TStructType::TOwnedMember> members;
        for (int fieldIndex = 0; fieldIndex < messageDescriptor.field_count(); ++fieldIndex) {
            const auto& innerFieldDescriptor = *messageDescriptor.field(fieldIndex);
            auto type = GetFieldType(
                innerFieldDescriptor,
                embeddedMessageDefaultOptions);

            if (HoldsAlternative<TOtherColumns>(type)) {
                ythrow TApiUsageError() <<
                    "Could not deduce YT type for field " << innerFieldDescriptor.name() << " of " <<
                    "embedded message field " << fieldDescriptor.name() << " " <<
                    "(note that " << EWrapperFieldFlag::OTHER_COLUMNS << " fields " <<
                    "are not allowed inside embedded messages)";
            } else if (HoldsAlternative<NTi::TTypePtr>(type)) {
                members.push_back(NTi::TStructType::TOwnedMember(
                    GetColumnName(innerFieldDescriptor),
                    Get<NTi::TTypePtr>(type)));
            } else {
                Y_FAIL();
            }
        }
        type = NTi::Struct(std::move(members));
    } else {
        auto scalarType = GetScalarFieldType(fieldDescriptor, options);
        if (HoldsAlternative<TOtherColumns>(scalarType)) {
            return TOtherColumns{};
        } else if (HoldsAlternative<EValueType>(scalarType)) {
            type = ToTypeV3(Get<EValueType>(scalarType), true);
        } else {
            Y_FAIL();
        }
    }

    switch (fieldDescriptor.label()) {
        case FieldDescriptor::Label::LABEL_REPEATED:
            Y_ENSURE(options.SerializationMode == EProtobufSerializationMode::Yt,
                "Repeated fields are supported only for YT serialization mode");
            switch (options.ListMode) {
                case EListMode::Required:
                    return NTi::TTypePtr(NTi::List(std::move(type)));
                case EListMode::Optional:
                    return NTi::TTypePtr(NTi::Optional(NTi::List(std::move(type))));
            }
            Y_FAIL();
        case FieldDescriptor::Label::LABEL_OPTIONAL:
            return NTi::TTypePtr(NTi::Optional(std::move(type)));
        case FieldDescriptor::LABEL_REQUIRED:
            return type;
    }
    Y_FAIL();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTableSchema CreateTableSchema(
    const Descriptor& messageDescriptor,
    bool keepFieldsWithoutExtension)
{
    TTableSchema result;

    TProtobufFieldOptions defaultOptions;
    ParseProtobufFieldOptions(
        messageDescriptor.options().GetRepeatedExtension(default_field_flags),
        &defaultOptions);
    for (int fieldIndex = 0; fieldIndex < messageDescriptor.field_count(); ++fieldIndex) {
        const auto& fieldDescriptor = *messageDescriptor.field(fieldIndex);
        if (!keepFieldsWithoutExtension && !HasNameExtension(fieldDescriptor)) {
            continue;
        }

        auto type = GetFieldType(fieldDescriptor, defaultOptions);
        if (HoldsAlternative<TOtherColumns>(type)) {
            result.Strict(false);
        } else if (HoldsAlternative<NTi::TTypePtr>(type)) {
            TColumnSchema column;
            column.Name(GetColumnName(fieldDescriptor));
            column.Type(std::move(Get<NTi::TTypePtr>(type)));
            result.AddColumn(std::move(column));
        } else {
            Y_FAIL();
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TFormat::TFormat(const TNode& config)
    : Config(config)
{ }


TFormat TFormat::Protobuf(const TVector<const ::google::protobuf::Descriptor*>& descriptors)
{
    return TFormat(MakeProtoFormatConfig(descriptors));
}

TFormat TFormat::YsonText()
{
    TNode config("yson");
    config.Attributes()("format", "text");
    return TFormat(config);
}

TFormat TFormat::YsonBinary()
{
    TNode config("yson");
    config.Attributes()("format", "binary");
    return TFormat(config);
}

TFormat TFormat::YaMRLenval()
{
    TNode config("yamr");
    config.Attributes()("lenval", true)("has_subkey", true);
    return TFormat(config);
}

TFormat TFormat::Json()
{
    return TFormat(TNode("json"));
}

bool TFormat::IsTextYson() const
{
    if (!Config.IsString() || Config.AsString() != "yson") {
        return false;
    }
    if (!Config.HasAttributes()) {
        return false;
    }
    const auto& attributes = Config.GetAttributes();
    if (!attributes.HasKey("format") || attributes["format"] != TNode("text")) {
        return false;
    }
    return true;
}

bool TFormat::IsProtobuf() const
{
    return Config.IsString() && Config.AsString() == "protobuf";
}

bool TFormat::IsYamredDsv() const
{
    return Config.IsString() && Config.AsString() == "yamred_dsv";
}

static TString FormatName(const TFormat& format)
{
    if (!format.Config.IsString()) {
        Y_VERIFY(format.Config.IsUndefined());
        return "<undefined>";
    }
    return format.Config.AsString();
}

TYamredDsvAttributes TFormat::GetYamredDsvAttributes() const
{
    if (!IsYamredDsv()) {
        ythrow TApiUsageError() << "Cannot get yamred_dsv attributes for " << FormatName(*this) << " format";
    }
    TYamredDsvAttributes attributes;

    const auto& nodeAttributes = Config.GetAttributes();
    {
        const auto& keyColumns = nodeAttributes["key_column_names"];
        if (!keyColumns.IsList()) {
            ythrow yexception() << "Ill-formed format: key_column_names is of non-list type: " << keyColumns.GetType();
        }
        for (auto& column : keyColumns.AsList()) {
            if (!column.IsString()) {
                ythrow yexception() << "Ill-formed format: key_column_names: " << column.GetType();
            }
            attributes.KeyColumnNames.push_back(column.AsString());
        }
    }

    if (nodeAttributes.HasKey("subkey_column_names")) {
        const auto& subkeyColumns = nodeAttributes["subkey_column_names"];
        if (!subkeyColumns.IsList()) {
            ythrow yexception() << "Ill-formed format: subkey_column_names is not a list: " << subkeyColumns.GetType();
        }
        for (const auto& column : subkeyColumns.AsList()) {
            if (!column.IsString()) {
                ythrow yexception() << "Ill-formed format: non-string inside subkey_key_column_names: " << column.GetType();
            }
            attributes.SubkeyColumnNames.push_back(column.AsString());
        }
    }

    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NYT::EWrapperFieldFlag::Enum>(IOutputStream& stream, NYT::EWrapperFieldFlag::Enum value)
{
    stream << NYT::EWrapperFieldFlag_Enum_Name(value);
}

