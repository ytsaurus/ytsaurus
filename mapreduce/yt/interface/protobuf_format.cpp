#include "protobuf_format.h"

#include "errors.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/stack.h>

#include <util/stream/output.h>


namespace NYT::NDetail {

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;

////////////////////////////////////////////////////////////////////////////////

using TFieldOption = TVariant<
    EProtobufType,
    EProtobufSerializationMode,
    EProtobufListMode,
    EProtobufMapMode>;

using TMessageOption = TVariant<
    EProtobufFieldSortOrder>;

TFieldOption FieldFlagToOption(EWrapperFieldFlag::Enum flag)
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
            return EProtobufListMode::Optional;
        case EFlag::REQUIRED_LIST:
            return EProtobufListMode::Required;

        case EFlag::MAP_AS_LIST_OF_STRUCTS_LEGACY:
            return EProtobufMapMode::ListOfStructsLegacy;
        case EFlag::MAP_AS_LIST_OF_STRUCTS:
            return EProtobufMapMode::ListOfStructs;
        case EFlag::MAP_AS_DICT:
            return EProtobufMapMode::Dict;
        case EFlag::MAP_AS_OPTIONAL_DICT:
            return EProtobufMapMode::OptionalDict;
    }
    Y_FAIL();
}

TMessageOption MessageFlagToOption(EWrapperMessageFlag::Enum flag)
{
    using EFlag = EWrapperMessageFlag;
    switch (flag) {
        case EFlag::DEPRECATED_SORT_FIELDS_AS_IN_PROTO_FILE:
            return EProtobufFieldSortOrder::AsInProtoFile;
        case EFlag::SORT_FIELDS_BY_FIELD_NUMBER:
            return EProtobufFieldSortOrder::ByFieldNumber;
    }
    Y_FAIL();
}

EWrapperFieldFlag::Enum OptionToFieldFlag(TFieldOption option)
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
        EFlag::Enum operator() (EProtobufListMode listMode)
        {
            switch (listMode) {
                case EProtobufListMode::Optional:
                    return EFlag::OPTIONAL_LIST;
                case EProtobufListMode::Required:
                    return EFlag::REQUIRED_LIST;
            }
            Y_FAIL();
        }
        EFlag::Enum operator() (EProtobufMapMode mapMode)
        {
            switch (mapMode) {
                case EProtobufMapMode::ListOfStructsLegacy:
                    return EFlag::MAP_AS_LIST_OF_STRUCTS_LEGACY;
                case EProtobufMapMode::ListOfStructs:
                    return EFlag::MAP_AS_LIST_OF_STRUCTS;
                case EProtobufMapMode::Dict:
                    return EFlag::MAP_AS_DICT;
                case EProtobufMapMode::OptionalDict:
                    return EFlag::MAP_AS_OPTIONAL_DICT;
            }
            Y_FAIL();
        }
    };

    return Visit(TVisitor(), option);
}

EWrapperMessageFlag::Enum OptionToMessageFlag(TMessageOption option)
{
    using EFlag = EWrapperMessageFlag;
    struct TVisitor
    {
        EFlag::Enum operator() (EProtobufFieldSortOrder sortOrder)
        {
            switch (sortOrder) {
                case EProtobufFieldSortOrder::AsInProtoFile:
                    return EFlag::DEPRECATED_SORT_FIELDS_AS_IN_PROTO_FILE;
                case EProtobufFieldSortOrder::ByFieldNumber:
                    return EFlag::SORT_FIELDS_BY_FIELD_NUMBER;
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

    void operator() (EProtobufListMode listMode)
    {
        SetOption(ListMode, listMode);
    }

    void operator() (EProtobufMapMode mapMode)
    {
        SetOption(MapMode, mapMode);
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
    TMaybe<EProtobufListMode> ListMode;
    TMaybe<EProtobufMapMode> MapMode;
};

class TParseProtobufMessageOptionsVisitor
{
public:
    void operator() (EProtobufFieldSortOrder fieldSortOrder)
    {
        SetOption(FieldSortOrder, fieldSortOrder);
    }

    template <typename T>
    void SetOption(TMaybe<T>& option, T newOption) {
        if (option) {
            if (*option == newOption) {
                ythrow yexception() << "Duplicate protobuf message flag " << OptionToMessageFlag(newOption);
            } else {
                ythrow yexception() << "Incompatible protobuf message flags " <<
                    OptionToMessageFlag(*option) << " and " << OptionToMessageFlag(newOption);
            }
        }
        option = newOption;
    };

public:
    TMaybe<EProtobufFieldSortOrder> FieldSortOrder;
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
    if (visitor.MapMode) {
        fieldOptions->MapMode = *visitor.MapMode;
    }
}

void ParseProtobufMessageOptions(
    const ::google::protobuf::RepeatedField<EWrapperMessageFlag::Enum>& flags,
    TProtobufMessageOptions* messageOptions)
{
    TParseProtobufMessageOptionsVisitor visitor;
    for (auto flag : flags) {
        Visit(visitor, MessageFlagToOption(flag));
    }
    if (visitor.FieldSortOrder) {
        messageOptions->FieldSortOrder = *visitor.FieldSortOrder;
    }
}

void ValidateProtobufType(const FieldDescriptor& fieldDescriptor, EProtobufType protobufType)
{
    const auto fieldType = fieldDescriptor.type();
    auto ensureType = [&] (FieldDescriptor::Type expectedType) {
        Y_ENSURE(fieldType == expectedType,
            "Type of field " << fieldDescriptor.name() << "does not match specified field flag " <<
            OptionToFieldFlag(protobufType) << ": "
            "expected " << FieldDescriptor::TypeName(expectedType) << ", " <<
            "got " << FieldDescriptor::TypeName(fieldType));
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

class TCycleChecker
{
private:
    class TGuard
    {
    public:
        TGuard(TCycleChecker* checker, const Descriptor* descriptor)
            : Checker_(checker)
            , Descriptor_(descriptor)
        {
            Checker_->ActiveVertices_.insert(Descriptor_);
            Checker_->Stack_.push(Descriptor_);
        }

        ~TGuard()
        {
            Checker_->ActiveVertices_.erase(Descriptor_);
            Checker_->Stack_.pop();
        }

    private:
        TCycleChecker* Checker_;
        const Descriptor* Descriptor_;
    };

public:
    [[nodiscard]] TGuard Enter(const Descriptor* descriptor)
    {
        if (ActiveVertices_.contains(descriptor)) {
            Y_VERIFY(!Stack_.empty());
            ythrow TApiUsageError() << "Cyclic reference found for protobuf messages. " <<
                "Consider removing " << EWrapperFieldFlag::SERIALIZATION_YT << " flag " <<
                "somewhere on the cycle containing " <<
                Stack_.top()->full_name() << " and " << descriptor->full_name();
        }
        return TGuard(this, descriptor);
    }

private:
    THashSet<const Descriptor*> ActiveVertices_;
    TStack<const Descriptor*> Stack_;
};

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
                    return "message";
                case EProtobufSerializationMode::Yt:
                    return "structured_message";
            }
            Y_FAIL();
        default:
            return fieldDescriptor->type_name();
    }
    Y_FAIL();
}

TString GetColumnName(const ::google::protobuf::FieldDescriptor& field)
{
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

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations,
    TCycleChecker& cycleChecker);

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations,
    const TProtobufFieldOptions& defaultOptions,
    TCycleChecker& cycleChecker);

TNode MakeMapFieldsConfig(
    const FieldDescriptor* fieldDescriptor,
    TNode* enumerations,
    const TProtobufFieldOptions& fieldOptions,
    TCycleChecker& cycleChecker)
{
    Y_VERIFY(fieldDescriptor->is_map());
    auto message = fieldDescriptor->message_type();
    switch (fieldOptions.MapMode) {
        case EProtobufMapMode::ListOfStructsLegacy:
            return MakeProtoFormatMessageFieldsConfig(message, enumerations, cycleChecker);
        case EProtobufMapMode::ListOfStructs:
        case EProtobufMapMode::Dict:
        case EProtobufMapMode::OptionalDict: {
            TProtobufFieldOptions options;
            options.SerializationMode = EProtobufSerializationMode::Yt;
            return MakeProtoFormatMessageFieldsConfig(message, enumerations, options, cycleChecker);
        }
    }
    Y_FAIL();
}

TNode MakeProtoFormatFieldConfig(
    const FieldDescriptor* fieldDescriptor,
    TNode* enumerations,
    const TProtobufFieldOptions& defaultOptions,
    TCycleChecker& cycleChecker)
{
    auto fieldConfig = TNode::CreateMap();
    fieldConfig["field_number"] = fieldDescriptor->number();
    fieldConfig["name"] = GetColumnName(*fieldDescriptor);

    auto fieldOptions = defaultOptions;
    ParseProtobufFieldOptions(fieldDescriptor->options().GetRepeatedExtension(flags), &fieldOptions);

    if (fieldDescriptor->is_repeated()) {
        Y_ENSURE_EX(fieldOptions.SerializationMode == EProtobufSerializationMode::Yt,
            TApiUsageError() << "Repeated field " << fieldDescriptor->full_name() << ' ' <<
            "must have flag " << EWrapperFieldFlag::SERIALIZATION_YT);
    }
    fieldConfig["repeated"] = fieldDescriptor->is_repeated();
    fieldConfig["packed"] = fieldDescriptor->is_packed();

    fieldConfig["proto_type"] = DeduceProtobufType(fieldDescriptor, fieldOptions);

    if (fieldDescriptor->type() == FieldDescriptor::TYPE_ENUM) {
        auto* enumeration = fieldDescriptor->enum_type();
        (*enumerations)[enumeration->name()] = MakeEnumerationConfig(enumeration);
        fieldConfig["enumeration_name"] = enumeration->name();
    }

    if (fieldOptions.SerializationMode != EProtobufSerializationMode::Yt) {
        return fieldConfig;
    }

    if (fieldDescriptor->is_map()) {
        fieldConfig["fields"] = MakeMapFieldsConfig(fieldDescriptor, enumerations, fieldOptions, cycleChecker);
        return fieldConfig;
    }

    if (fieldDescriptor->type() == FieldDescriptor::TYPE_MESSAGE) {
        fieldConfig["fields"] = MakeProtoFormatMessageFieldsConfig(
            fieldDescriptor->message_type(),
            enumerations,
            cycleChecker);
    }

    return fieldConfig;
}

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations,
    const TProtobufFieldOptions& defaultOptions,
    TCycleChecker& cycleChecker)
{
    TNode fields = TNode::CreateList();
    auto guard = cycleChecker.Enter(descriptor);
    for (int fieldIndex = 0; fieldIndex < descriptor->field_count(); ++fieldIndex) {
        auto* fieldDesc = descriptor->field(fieldIndex);
        fields.Add(MakeProtoFormatFieldConfig(
            fieldDesc,
            enumerations,
            defaultOptions,
            cycleChecker));
    }
    return fields;
}

TNode MakeProtoFormatMessageFieldsConfig(
    const Descriptor* descriptor,
    TNode* enumerations,
    TCycleChecker& cycleChecker)
{
    TProtobufFieldOptions defaultOptions;
    ParseProtobufFieldOptions(descriptor->options().GetRepeatedExtension(default_field_flags), &defaultOptions);
    return MakeProtoFormatMessageFieldsConfig(descriptor, enumerations, defaultOptions, cycleChecker);
}

TNode MakeProtoFormatConfig(const TVector<const Descriptor*>& descriptors)
{
    TNode config("protobuf");
    config.Attributes()
        ("enumerations", TNode::CreateMap())
        ("tables", TNode::CreateList());

    auto& enumerations = config.Attributes()["enumerations"];

    for (auto* descriptor : descriptors) {
        TCycleChecker cycleChecker;
        auto columns = MakeProtoFormatMessageFieldsConfig(descriptor, &enumerations, cycleChecker);
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

void SortFields(TVector<const FieldDescriptor*>& fieldDescriptors, EProtobufFieldSortOrder fieldSortOrder)
{
    switch (fieldSortOrder) {
        case EProtobufFieldSortOrder::AsInProtoFile:
            return;
        case EProtobufFieldSortOrder::ByFieldNumber:
            SortBy(fieldDescriptors, [] (const FieldDescriptor* fieldDescriptor) {
                return fieldDescriptor->number();
            });
            return;
    }
    Y_FAIL();
}

TVariant<NTi::TTypePtr, TOtherColumns> GetFieldType(
    const FieldDescriptor& fieldDescriptor,
    const TProtobufFieldOptions& defaultOptions,
    TCycleChecker& cycleChecker);

NTi::TTypePtr GetMessageType(
    const FieldDescriptor& fieldDescriptor,
    const TProtobufFieldOptions& defaultFieldOptions,
    TCycleChecker& cycleChecker)
{
    const auto& messageDescriptor = *fieldDescriptor.message_type();
    auto guard = cycleChecker.Enter(&messageDescriptor);
    auto embeddedMessageDefaultFieldOptions = defaultFieldOptions;
    TProtobufMessageOptions embeddedMessageOptions;
    ParseProtobufFieldOptions(
        messageDescriptor.options().GetRepeatedExtension(default_field_flags),
        &embeddedMessageDefaultFieldOptions);
    ParseProtobufMessageOptions(
        messageDescriptor.options().GetRepeatedExtension(message_flags),
        &embeddedMessageOptions);

    TVector<const FieldDescriptor*> fieldDescriptors;
    fieldDescriptors.reserve(messageDescriptor.field_count());
    for (int i = 0; i < messageDescriptor.field_count(); ++i) {
        fieldDescriptors.push_back(messageDescriptor.field(i));
    }
    SortFields(fieldDescriptors, embeddedMessageOptions.FieldSortOrder);

    TVector<NTi::TStructType::TOwnedMember> members;
    for (const auto innerFieldDescriptor : fieldDescriptors) {
        auto type = GetFieldType(
            *innerFieldDescriptor,
            embeddedMessageDefaultFieldOptions,
            cycleChecker);

        if (HoldsAlternative<TOtherColumns>(type)) {
            ythrow TApiUsageError() <<
                "Could not deduce YT type for field " << innerFieldDescriptor->name() << " of " <<
                "embedded message field " << fieldDescriptor.name() << " " <<
                "(note that " << EWrapperFieldFlag::OTHER_COLUMNS << " fields " <<
                "are not allowed inside embedded messages)";
        } else if (HoldsAlternative<NTi::TTypePtr>(type)) {
            members.push_back(NTi::TStructType::TOwnedMember(
                GetColumnName(*innerFieldDescriptor),
                Get<NTi::TTypePtr>(type)));
        } else {
            Y_FAIL();
        }
    }
    return NTi::Struct(std::move(members));
}

NTi::TTypePtr GetMapType(
    const FieldDescriptor& fieldDescriptor,
    const TProtobufFieldOptions& fieldOptions,
    TCycleChecker& cycleChecker)
{
    Y_VERIFY(fieldDescriptor.is_map());
    switch (fieldOptions.MapMode) {
        case EProtobufMapMode::ListOfStructsLegacy:
        case EProtobufMapMode::ListOfStructs: {
            TProtobufFieldOptions embeddedOptions;
            if (fieldOptions.MapMode == EProtobufMapMode::ListOfStructs) {
                embeddedOptions.SerializationMode = EProtobufSerializationMode::Yt;
            }
            auto list = NTi::List(GetMessageType(fieldDescriptor, embeddedOptions, cycleChecker));
            switch (fieldOptions.ListMode) {
                case EProtobufListMode::Required:
                    return list;
                case EProtobufListMode::Optional:
                    return NTi::Optional(std::move(list));
            }
            Y_FAIL();
        }
        case EProtobufMapMode::Dict:
        case EProtobufMapMode::OptionalDict: {
            auto message = fieldDescriptor.message_type();
            Y_VERIFY(message->field_count() == 2);
            auto keyVariant = GetScalarFieldType(*message->field(0), TProtobufFieldOptions{});
            Y_VERIFY(HoldsAlternative<EValueType>(keyVariant));
            auto key = Get<EValueType>(keyVariant);
            TProtobufFieldOptions embeddedOptions;
            embeddedOptions.SerializationMode = EProtobufSerializationMode::Yt;
            auto valueVariant = GetFieldType(*message->field(1), embeddedOptions, cycleChecker);
            Y_VERIFY(HoldsAlternative<NTi::TTypePtr>(valueVariant));
            auto value = Get<NTi::TTypePtr>(valueVariant);
            Y_VERIFY(value->IsOptional());
            value = value->AsOptional()->GetItemType();
            auto dict = NTi::Dict(ToTypeV3(key, true), value);
            if (fieldOptions.MapMode == EProtobufMapMode::OptionalDict) {
                return NTi::Optional(dict);
            } else {
                return dict;
            }
        }
    }
}

TVariant<NTi::TTypePtr, TOtherColumns> GetFieldType(
    const FieldDescriptor& fieldDescriptor,
    const TProtobufFieldOptions& defaultOptions,
    TCycleChecker& cycleChecker)
{
    auto fieldOptions = defaultOptions;
    ParseProtobufFieldOptions(
        fieldDescriptor.options().GetRepeatedExtension(flags),
        &fieldOptions);

    if (fieldOptions.Type) {
        ValidateProtobufType(fieldDescriptor, *fieldOptions.Type);
    }

    NTi::TTypePtr type;
    if (fieldDescriptor.type() == FieldDescriptor::TYPE_MESSAGE &&
        fieldOptions.SerializationMode == EProtobufSerializationMode::Yt)
    {
        if (fieldDescriptor.is_map()) {
            return GetMapType(fieldDescriptor, fieldOptions, cycleChecker);
        } else {
            type = GetMessageType(fieldDescriptor, TProtobufFieldOptions{}, cycleChecker);
        }
    } else {
        auto scalarType = GetScalarFieldType(fieldDescriptor, fieldOptions);
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
            Y_ENSURE(fieldOptions.SerializationMode == EProtobufSerializationMode::Yt,
                "Repeated fields are supported only for YT serialization mode");
            switch (fieldOptions.ListMode) {
                case EProtobufListMode::Required:
                    return NTi::TTypePtr(NTi::List(std::move(type)));
                case EProtobufListMode::Optional:
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

TMaybe<TVector<TString>> InferColumnFilter(const ::google::protobuf::Descriptor& descriptor)
{
    auto isOtherColumns = [] (const ::google::protobuf::FieldDescriptor& field) {
        TProtobufFieldOptions options;
        ParseProtobufFieldOptions(
            field.options().GetRepeatedExtension(flags),
            &options);
        return options.Type == EProtobufType::OtherColumns;
    };

    TVector<TString> result;
    result.reserve(descriptor.field_count());
    for (int i = 0; i < descriptor.field_count(); ++i) {
        const auto& field = *descriptor.field(i);
        if (isOtherColumns(field)) {
            return {};
        }
        result.push_back(GetColumnName(field));
    }
    return result;
}

TTableSchema CreateTableSchemaImpl(
    const Descriptor& messageDescriptor,
    bool keepFieldsWithoutExtension)
{
    TTableSchema result;

    TProtobufFieldOptions defaultOptions;
    ParseProtobufFieldOptions(
        messageDescriptor.options().GetRepeatedExtension(default_field_flags),
        &defaultOptions);
    TCycleChecker cycleChecker;
    auto guard = cycleChecker.Enter(&messageDescriptor);
    for (int fieldIndex = 0; fieldIndex < messageDescriptor.field_count(); ++fieldIndex) {
        const auto& fieldDescriptor = *messageDescriptor.field(fieldIndex);
        if (!keepFieldsWithoutExtension && !HasNameExtension(fieldDescriptor)) {
            continue;
        }

        auto type = GetFieldType(fieldDescriptor, defaultOptions, cycleChecker);
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

} // namespace NYT::NDetail

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NYT::EWrapperFieldFlag::Enum>(IOutputStream& stream, NYT::EWrapperFieldFlag::Enum value)
{
    stream << NYT::EWrapperFieldFlag_Enum_Name(value);
}

template <>
void Out<NYT::EWrapperMessageFlag::Enum>(IOutputStream& stream, NYT::EWrapperMessageFlag::Enum value)
{
    stream << NYT::EWrapperMessageFlag_Enum_Name(value);
}

