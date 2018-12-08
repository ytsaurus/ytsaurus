#include "protobuf_interop.h"

#include <yt/core/yson/proto/protobuf_interop.pb.h>

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/writer.h>
#include <yt/core/yson/forwarding_consumer.h>
#include <yt/core/yson/null_consumer.h>

#include <yt/core/ypath/token.h>

#include <yt/core/misc/zigzag.h>
#include <yt/core/misc/varint.h>
#include <yt/core/misc/variant.h>
#include <yt/core/misc/cast.h>
#include <yt/core/misc/string.h>
#include <yt/core/misc/small_vector.h>

#include <yt/core/ytree/proto/attributes.pb.h>

#include <contrib/libs/protobuf/descriptor.h>
#include <contrib/libs/protobuf/wire_format.h>

#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NYson {

using namespace NYson;
using namespace NYTree;
using namespace NYPath;
using namespace google::protobuf;
using namespace google::protobuf::io;
using namespace google::protobuf::internal;

////////////////////////////////////////////////////////////////////////////////

class TProtobufField;
class TProtobufEnumType;

static constexpr size_t TypicalFieldCount = 16;
using TFieldNumberList = SmallVector<int, TypicalFieldCount>;

static constexpr int AttributeDictionaryAttributeFieldNumber = 1;
static constexpr int AttributeDictionaryKeyFieldNumber = 1;
static constexpr int AttributeDictionaryValueFieldNumber = 2;

static constexpr int ProtobufMapKeyFieldNumber = 1;
static constexpr int ProtobufMapValueFieldNumber = 2;

////////////////////////////////////////////////////////////////////////////////

namespace {

TString ToUnderscoreCase(const TString& protobufName)
{
    TStringBuilder builder;
    for (auto ch : protobufName) {
        if (isupper(ch)) {
            if (builder.GetLength() > 0 && builder.GetBuffer()[builder.GetLength() - 1] != '_') {
                builder.AppendChar('_');
            }
            builder.AppendChar(tolower(ch));
        } else {
            builder.AppendChar(ch);
        }
    }
    return builder.Flush();
}

TString DeriveYsonName(const TString& protobufName, const google::protobuf::FileDescriptor* fileDescriptor)
{
    if (fileDescriptor->options().GetExtension(NYT::NYson::NProto::derive_underscore_case_names)) {
        return ToUnderscoreCase(protobufName);
    } else {
        return protobufName;
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TProtobufTypeRegistry
{
public:
    TStringBuf GetYsonName(const FieldDescriptor* descriptor)
    {
        return GetYsonNameFromDescriptor(
            descriptor,
            descriptor->options().GetExtension(NYT::NYson::NProto::field_name));
    }

    TStringBuf GetYsonLiteral(const EnumValueDescriptor* descriptor)
    {
        return GetYsonNameFromDescriptor(
            descriptor,
            descriptor->options().GetExtension(NYT::NYson::NProto::enum_value_name));
    }

    const TProtobufMessageType* ReflectMessageType(const Descriptor* descriptor);
    const TProtobufEnumType* ReflectEnumType(const EnumDescriptor* descriptor);

    static TProtobufTypeRegistry* Get()
    {
        return Singleton<TProtobufTypeRegistry>();
    }

private:
    Y_DECLARE_SINGLETON_FRIEND();
    TProtobufTypeRegistry() = default;

    template <class TDescriptor>
    TStringBuf GetYsonNameFromDescriptor(const TDescriptor* descriptor, const TString& annotatedName)
    {
        auto ysonName = annotatedName ? annotatedName : DeriveYsonName(descriptor->name(), descriptor->file());
        return InternString(ysonName);
    }

    TStringBuf InternString(const TString& str)
    {
        auto guard = Guard(SpinLock_);
        InternedStrings_.push_back(str);
        return InternedStrings_.back();
    }

private:
    TSpinLock SpinLock_;
    std::vector<TString> InternedStrings_;
    THashMap<const Descriptor*, std::unique_ptr<TProtobufMessageType>> MessageTypeMap_;
    THashMap<const EnumDescriptor*, std::unique_ptr<TProtobufEnumType>> EnumTypeMap_;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufField
{
public:
    TProtobufField(TProtobufTypeRegistry* registry, const FieldDescriptor* descriptor)
        : Underlying_(descriptor)
        , YsonName_(registry->GetYsonName(descriptor))
        , MessageType_(descriptor->type() == FieldDescriptor::TYPE_MESSAGE ? registry->ReflectMessageType(
            descriptor->message_type()) : nullptr)
        , EnumType_(descriptor->type() == FieldDescriptor::TYPE_ENUM ? registry->ReflectEnumType(
            descriptor->enum_type()) : nullptr)
        , YsonString_(descriptor->options().GetExtension(NYT::NYson::NProto::yson_string))
        , YsonMap_(descriptor->options().GetExtension(NYT::NYson::NProto::yson_map))
    {
        if (YsonMap_ && !descriptor->is_map()) {
            THROW_ERROR_EXCEPTION("Field %v is not a map and cannot be annotated with \"yson_\" option",
                GetFullName());
        }

        if (YsonMap_) {
            const auto* keyField = descriptor->message_type()->FindFieldByNumber(ProtobufMapKeyFieldNumber);
            if (keyField->type() != FieldDescriptor::TYPE_STRING &&
                keyField->type() != FieldDescriptor::TYPE_BYTES)
            {
                THROW_ERROR_EXCEPTION("Map field %v has invalid key type",
                    GetFullName());
            }
        }
    }

    ui32 GetTag() const
    {
        return google::protobuf::internal::WireFormat::MakeTag(Underlying_);
    }

    const TString& GetFullName() const
    {
        return Underlying_->full_name();
    }

    TStringBuf GetYsonName() const
    {
        return YsonName_;
    }

    int GetNumber() const
    {
        return Underlying_->number();
    }

    FieldDescriptor::Type GetType() const
    {
        return Underlying_->type();
    }

    bool IsRepeated() const
    {
        return Underlying_->is_repeated();
    }

    bool IsRequired() const
    {
        return Underlying_->is_required();
    }

    bool IsOptional() const
    {
        return Underlying_->is_optional();
    }

    bool IsYsonString() const
    {
        return YsonString_;
    }

    bool IsYsonMap() const
    {
        return YsonMap_;
    }

    const TProtobufMessageType* GetMessageType() const
    {
        return MessageType_;
    }

    const TProtobufEnumType* GetEnumType() const
    {
        return EnumType_;
    }

private:
    const FieldDescriptor* const Underlying_;
    const TStringBuf YsonName_;
    const TProtobufMessageType* MessageType_;
    const TProtobufEnumType* EnumType_;
    const bool YsonString_;
    const bool YsonMap_;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufMessageType
{
public:
    TProtobufMessageType(TProtobufTypeRegistry* registry, const Descriptor* descriptor)
        : Registry_(registry)
        , Underlying_(descriptor)
        , AttributeDictionary_(descriptor->options().GetExtension(NYT::NYson::NProto::attribute_dictionary))
    { }

    void Build()
    {
        for (int index = 0; index < Underlying_->field_count(); ++index) {
            const auto* fieldDescriptor = Underlying_->field(index);
            auto fieldHolder = std::make_unique<TProtobufField>(Registry_, fieldDescriptor);
            auto* field = fieldHolder.get();
            if (field->IsRequired()) {
                RequiredFieldNumbers_.push_back(field->GetNumber());
            }
            YCHECK(NameToField_.emplace(field->GetYsonName(), std::move(fieldHolder)).second);
            YCHECK(NumberToField_.emplace(field->GetNumber(), field).second);
        }
    }

    bool IsAttributeDictionary() const
    {
        return AttributeDictionary_;
    }

    const TString& GetFullName() const
    {
        return Underlying_->full_name();
    }

    const std::vector<int>& GetRequiredFieldNumbers() const
    {
        return RequiredFieldNumbers_;
    }

    const TProtobufField* FindFieldByName(TStringBuf name) const
    {
        auto it = NameToField_.find(name);
        return it == NameToField_.end() ? nullptr : it->second.get();
    }

    const TProtobufField* FindFieldByNumber(int number) const
    {
        auto it = NumberToField_.find(number);
        return it == NumberToField_.end() ? nullptr : it->second;
    }

    const TProtobufField* GetFieldByNumber(int number) const
    {
        const auto* field = FindFieldByNumber(number);
        YCHECK(field);
        return field;
    }

private:
    TProtobufTypeRegistry* const Registry_;
    const Descriptor* const Underlying_;
    const bool AttributeDictionary_;

    std::vector<int> RequiredFieldNumbers_;
    THashMap<TStringBuf, std::unique_ptr<TProtobufField>> NameToField_;
    THashMap<int, const TProtobufField*> NumberToField_;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufEnumType
{
public:
    TProtobufEnumType(TProtobufTypeRegistry* registry, const EnumDescriptor* descriptor)
        : Registry_(registry)
        , Underlying_(descriptor)
    { }

    void Build()
    {
        for (int index = 0; index < Underlying_->value_count(); ++index) {
            const auto* valueDescriptor = Underlying_->value(index);
            auto literal = Registry_->GetYsonLiteral(valueDescriptor);
            YCHECK(LiteralToValue_.emplace(literal, valueDescriptor->number()).second);
            YCHECK(ValueToLiteral_.emplace(valueDescriptor->number(), literal).second);
        }
    }

    const TString& GetFullName() const
    {
        return Underlying_->full_name();
    }

    std::optional<int> FindValueByLiteral(TStringBuf literal) const
    {
        auto it = LiteralToValue_.find(literal);
        return it == LiteralToValue_.end() ? std::nullopt : std::make_optional(it->second);
    }

    TStringBuf FindLiteralByValue(int value) const
    {
        auto it = ValueToLiteral_.find(value);
        return it == ValueToLiteral_.end() ? TStringBuf() : it->second;
    }

private:
    TProtobufTypeRegistry* const Registry_;
    const EnumDescriptor* const Underlying_;

    THashMap<TStringBuf, int> LiteralToValue_;
    THashMap<int, TStringBuf> ValueToLiteral_;
};

////////////////////////////////////////////////////////////////////////////////

const TProtobufMessageType* TProtobufTypeRegistry::ReflectMessageType(const Descriptor* descriptor)
{
    auto guard = Guard(SpinLock_);
    auto it = MessageTypeMap_.find(descriptor);
    if (it != MessageTypeMap_.end()) {
        return it->second.get();
    }
    auto typeHolder = std::make_unique<TProtobufMessageType>(this, descriptor);
    auto* type = typeHolder.get();
    it = MessageTypeMap_.emplace(descriptor, std::move(typeHolder)).first;
    guard.Release();
    type->Build();
    return type;
}

const TProtobufEnumType* TProtobufTypeRegistry::ReflectEnumType(const EnumDescriptor* descriptor)
{
    auto guard = Guard(SpinLock_);
    auto it = EnumTypeMap_.find(descriptor);
    if (it != EnumTypeMap_.end()) {
        return it->second.get();
    }
    auto typeHolder = std::make_unique<TProtobufEnumType>(this, descriptor);
    auto* type = typeHolder.get();
    it = EnumTypeMap_.emplace(descriptor, std::move(typeHolder)).first;
    guard.Release();
    type->Build();
    return type;
}

////////////////////////////////////////////////////////////////////////////////

const TProtobufMessageType* ReflectProtobufMessageType(const Descriptor* descriptor)
{
    return TProtobufTypeRegistry::Get()->ReflectMessageType(descriptor);
}

////////////////////////////////////////////////////////////////////////////////

class TYPathStack
{
public:
    void Push(const TProtobufField* field)
    {
        Items_.push_back(field);
    }

    void Push(TString key)
    {
        Items_.emplace_back(std::move(key));
    }

    void Push(int index)
    {
        Items_.push_back(index);
    }

    void Pop()
    {
        Items_.pop_back();
    }

    bool IsEmpty() const
    {
        return Items_.empty();
    }

    TYPath GetPath() const
    {
        if (Items_.empty()) {
            return "/";
        }
        TStringBuilder builder;
        for (const auto& item : Items_) {
            builder.AppendChar('/');
            switch (item.Tag()) {
                case TEntry::TagOf<const TProtobufField*>():
                    builder.AppendString(ToYPathLiteral(item.As<const TProtobufField*>()->GetYsonName()));
                    break;
                case TEntry::TagOf<TString>():
                    builder.AppendString(ToYPathLiteral(item.As<TString>()));
                    break;
                case TEntry::TagOf<int>():
                    builder.AppendFormat("%v", item.As<int>());
                    break;
                default:
                    Y_UNREACHABLE();
            }
        }
        return builder.Flush();
    }

private:
    using TEntry = TVariant<
        const TProtobufField*,
        TString,
        int>;
    std::vector<TEntry> Items_;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufTranscoderBase
{
protected:
    TYPathStack YPathStack_;


    void SortFields(TFieldNumberList* numbers)
    {
        std::sort(numbers->begin(), numbers->end());
    }

    void ValidateRequiredFieldsPresent(const TProtobufMessageType* type, const TFieldNumberList& numbers)
    {
        if (numbers.size() == type->GetRequiredFieldNumbers().size()) {
            return;
        }

        for (auto number : type->GetRequiredFieldNumbers()) {
            if (!std::binary_search(numbers.begin(), numbers.end(), number)) {
                const auto* field = type->FindFieldByNumber(number);
                YCHECK(field);
                YPathStack_.Push(field);
                THROW_ERROR_EXCEPTION("Missing required field %v",
                    YPathStack_.GetPath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("protobuf_type", type->GetFullName())
                    << TErrorAttribute("protobuf_field", field->GetFullName());
            }
        }

        Y_UNREACHABLE();
    }

    void ValidateNoFieldDuplicates(const TProtobufMessageType* type, const TFieldNumberList& numbers)
    {
        for (auto index = 0; index + 1 < numbers.size(); ++index) {
            if (numbers[index] == numbers[index + 1]) {
                const auto* field = type->GetFieldByNumber(numbers[index]);
                YPathStack_.Push(field);
                THROW_ERROR_EXCEPTION("Duplicate field %v",
                    YPathStack_.GetPath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("protobuf_type", type->GetFullName());
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufWriter
    : public TProtobufTranscoderBase
    , public TForwardingYsonConsumer
{
public:
    TProtobufWriter(
        ZeroCopyOutputStream* outputStream,
        const TProtobufMessageType* rootType,
        const TProtobufWriterOptions& options)
        : OutputStream_(outputStream)
        , RootType_(rootType)
        , Options_(options)
        , BodyOutputStream_(&BodyString_)
        , BodyCodedStream_(&BodyOutputStream_)
        , AttributeValueStream_(AttributeValue_)
        , AttributeValueWriter_(&AttributeValueStream_)
        , YsonStringStream_(YsonString_)
        , YsonStringWriter_(&YsonStringStream_)
    { }

private:
    ZeroCopyOutputStream* const OutputStream_;
    const TProtobufMessageType* const RootType_;
    const TProtobufWriterOptions Options_;

    TString BodyString_;
    google::protobuf::io::StringOutputStream BodyOutputStream_;
    google::protobuf::io::CodedOutputStream BodyCodedStream_;

    struct TTypeEntry
    {
        explicit TTypeEntry(const TProtobufMessageType* type)
            : Type(type)
        { }

        const TProtobufMessageType* Type;
        TFieldNumberList RequiredFieldNumbers;
        TFieldNumberList NonRequiredFieldNumbers;
        int CurrentMapIndex = 0;
    };
    std::vector<TTypeEntry> TypeStack_;

    std::vector<int> NestedIndexStack_;

    struct TFieldEntry
    {
        TFieldEntry(
            const TProtobufField* field,
            int currentListIndex,
            bool inList)
            : Field(field)
            , CurrentListIndex(currentListIndex)
            , InList(inList)
        { }

        const TProtobufField* Field;
        int CurrentListIndex;
        bool InList;
    };
    std::vector<TFieldEntry> FieldStack_;

    struct TNestedMessageEntry
    {
        TNestedMessageEntry(int lo, int hi)
            : Lo(lo)
            , Hi(hi)
        { }

        int Lo;
        int Hi;
        int ByteSize = -1;
    };
    std::vector<TNestedMessageEntry> NestedMessages_;

    TString AttributeKey_;
    TString AttributeValue_;
    TStringOutput AttributeValueStream_;
    TBufferedBinaryYsonWriter AttributeValueWriter_;

    TString YsonString_;
    TStringOutput YsonStringStream_;
    TBufferedBinaryYsonWriter YsonStringWriter_;

    virtual void OnMyStringScalar(TStringBuf value) override
    {
        WriteScalar([&] {
            const auto* field = FieldStack_.back().Field;
            switch (field->GetType()) {
                case FieldDescriptor::TYPE_STRING:
                case FieldDescriptor::TYPE_BYTES:
                    BodyCodedStream_.WriteVarint64(value.length());
                    BodyCodedStream_.WriteRaw(value.begin(), static_cast<int>(value.length()));
                    break;

                case FieldDescriptor::TYPE_ENUM: {
                    const auto* enumType = field->GetEnumType();
                    auto optionalValue = enumType->FindValueByLiteral(value);
                    if (!optionalValue) {
                        THROW_ERROR_EXCEPTION("Field %v cannot have value %Qv",
                            YPathStack_.GetPath(),
                            value)
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_type", enumType->GetFullName());
                    }
                    BodyCodedStream_.WriteVarint32SignExtended(*optionalValue);
                    break;
                }

                default:
                    THROW_ERROR_EXCEPTION("Field %v cannot be parsed from \"string\" values",
                        YPathStack_.GetPath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
            }
        });
    }

    virtual void OnMyInt64Scalar(i64 value) override
    {
        OnIntegerScalar(value);
    }

    virtual void OnMyUint64Scalar(ui64 value) override
    {
        OnIntegerScalar(value);
    }

    virtual void OnMyDoubleScalar(double value) override
    {
        WriteScalar([&] {
            const auto* field = FieldStack_.back().Field;
            switch (field->GetType()) {
                case FieldDescriptor::TYPE_DOUBLE: {
                    auto encodedValue = WireFormatLite::EncodeDouble(value);
                    BodyCodedStream_.WriteRaw(&encodedValue, sizeof(encodedValue));
                    break;
                }

                case FieldDescriptor::TYPE_FLOAT: {
                    auto encodedValue = WireFormatLite::EncodeFloat(value);
                    BodyCodedStream_.WriteRaw(&encodedValue, sizeof(encodedValue));
                    break;
                }

                default:
                    THROW_ERROR_EXCEPTION("Field %v cannot be parsed from \"double\" values",
                        YPathStack_.GetPath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
            }
        });
    }

    virtual void OnMyBooleanScalar(bool value) override
    {
        WriteScalar([&] {
            const auto* field = FieldStack_.back().Field;
            auto type = field->GetType();
            if (type != FieldDescriptor::TYPE_BOOL) {
                THROW_ERROR_EXCEPTION("Field %v cannot be parsed from \"boolean\" values",
                    YPathStack_.GetPath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_field", field->GetFullName());
            }
            BodyCodedStream_.WriteVarint32(value ? 1 : 0);
        });
    }

    virtual void OnMyEntity() override
    {
        if (FieldStack_.empty()) {
            // This is the root.
            return;
        }
        ValidateNotRepeated();
        FieldStack_.pop_back();
        YPathStack_.Pop();
    }

    virtual void OnMyBeginList() override
    {
        ValidateNotRoot();

        const auto* field = FieldStack_.back().Field;
        if (field->IsYsonMap()) {
            THROW_ERROR_EXCEPTION("Map %v cannot be parsed from \"list\" values",
                YPathStack_.GetPath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_field", field->GetFullName());
        }

        ValidateRepeated();
    }

    virtual void OnMyListItem() override
    {
        Y_ASSERT(!TypeStack_.empty());
        const auto* field = FieldStack_.back().Field;
        int index = FieldStack_.back().CurrentListIndex++;
        FieldStack_.emplace_back(field, index, true);
        YPathStack_.Push(index);
    }

    virtual void OnMyEndList() override
    {
        Y_ASSERT(!TypeStack_.empty());
        FieldStack_.pop_back();
        YPathStack_.Pop();
    }

    virtual void OnMyBeginMap() override
    {
        if (TypeStack_.empty()) {
            TypeStack_.emplace_back(RootType_);
            FieldStack_.emplace_back(nullptr, 0, false);
            return;
        }

        const auto* field = FieldStack_.back().Field;
        TypeStack_.emplace_back(field->GetMessageType());
        if (!field->IsYsonMap()) {
            if (field->GetType() != FieldDescriptor::TYPE_MESSAGE) {
                THROW_ERROR_EXCEPTION("Field %v cannot be parsed from \"map\" values",
                    YPathStack_.GetPath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_field", field->GetFullName());
            }

            ValidateNotRepeated();
            WriteTag();
            BeginNestedMessage();
        }
    }

    virtual void OnMyKeyedItem(TStringBuf key) override
    {
        const auto* field = FieldStack_.back().Field;
        if (field && field->IsYsonMap()) {
            OnMyKeyedItemYsonMap(key);
        } else {
            Y_ASSERT(!TypeStack_.empty());
            const auto* type = TypeStack_.back().Type;
            if (type->IsAttributeDictionary()) {
                OnMyKeyedItemAttributeDictionary(key);
            } else {
                OnMyKeyedItemRegular(key);
            }
        }
    }

    void OnMyKeyedItemYsonMap(TStringBuf key)
    {
        auto& typeEntry = TypeStack_.back();
        if (typeEntry.CurrentMapIndex > 0) {
            EndNestedMessage();
        }
        ++typeEntry.CurrentMapIndex;

        WriteTag();
        BeginNestedMessage();

        BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(
            ProtobufMapKeyFieldNumber,
            WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        BodyCodedStream_.WriteVarint64(key.length());
        BodyCodedStream_.WriteRaw(key.data(), static_cast<int>(key.length()));

        const auto* field = FieldStack_.back().Field;
        const auto* valueField = field->GetMessageType()->GetFieldByNumber(ProtobufMapValueFieldNumber);
        FieldStack_.emplace_back(valueField, 0, false);
        YPathStack_.Push(TString(key));
    }

    void OnMyKeyedItemRegular(TStringBuf key)
    {
        const auto* type = TypeStack_.back().Type;
        const auto* field = type->FindFieldByName(key);
        if (!field) {
            if (Options_.SkipUnknownFields) {
                Forward(GetNullYsonConsumer(), [] {});
                return;
            }
            THROW_ERROR_EXCEPTION("Unknown field %Qv at %v",
                key,
                YPathStack_.GetPath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_type", type->GetFullName());
        }

        auto number = field->GetNumber();
        auto& typeEntry = TypeStack_.back();
        ++typeEntry.CurrentMapIndex;
        if (field->IsRequired()) {
            typeEntry.RequiredFieldNumbers.push_back(number);
        } else {
            typeEntry.NonRequiredFieldNumbers.push_back(number);
        }
        FieldStack_.emplace_back(field, 0, false);
        YPathStack_.Push(field);

        if (field->IsYsonString()) {
            YsonString_.clear();
            Forward(&YsonStringWriter_, [this] {
                YsonStringWriter_.Flush();

                WriteScalar([this] {
                    BodyCodedStream_.WriteVarint64(YsonString_.length());
                    BodyCodedStream_.WriteRaw(YsonString_.begin(), static_cast<int>(YsonString_.length()));
                });
            });
        }
    }

    void OnMyKeyedItemAttributeDictionary(TStringBuf key)
    {
        AttributeKey_ = key;
        AttributeValue_.clear();
        Forward(&AttributeValueWriter_, [this] {
            AttributeValueWriter_.Flush();

            BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(AttributeDictionaryAttributeFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
            BodyCodedStream_.WriteVarint64(
                1 +
                CodedOutputStream::VarintSize64(AttributeKey_.length()) +
                AttributeKey_.length() +
                1 +
                CodedOutputStream::VarintSize64(AttributeValue_.length()) +
                AttributeValue_.length());

            BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(AttributeDictionaryKeyFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
            BodyCodedStream_.WriteVarint64(AttributeKey_.length());
            BodyCodedStream_.WriteRaw(AttributeKey_.data(), AttributeKey_.length());

            BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(AttributeDictionaryValueFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
            BodyCodedStream_.WriteVarint64(AttributeValue_.length());
            BodyCodedStream_.WriteRaw(AttributeValue_.data(), AttributeValue_.length());
        });
    }

    virtual void OnMyEndMap() override
    {
        auto& typeEntry = TypeStack_.back();
        const auto* type = typeEntry.Type;

        const auto* field = FieldStack_.back().Field;
        if (field && field->IsYsonMap()) {
            if (typeEntry.CurrentMapIndex > 0) {
                EndNestedMessage();
            }

            TypeStack_.pop_back();
        } else {
            SortFields(&typeEntry.NonRequiredFieldNumbers);
            ValidateNoFieldDuplicates(type, typeEntry.NonRequiredFieldNumbers);

            SortFields(&typeEntry.RequiredFieldNumbers);
            ValidateNoFieldDuplicates(type, typeEntry.RequiredFieldNumbers);

            if (!Options_.SkipRequiredFields) {
                ValidateRequiredFieldsPresent(type, typeEntry.RequiredFieldNumbers);
            }

            TypeStack_.pop_back();
            if (TypeStack_.empty()) {
                Finish();
                return;
            }

            EndNestedMessage();
        }

        FieldStack_.pop_back();
        YPathStack_.Pop();
    }

    void ThrowAttributesNotSupported()
    {
        THROW_ERROR_EXCEPTION("Attributes are not supported")
            << TErrorAttribute("ypath", YPathStack_.GetPath());
    }

    virtual void OnMyBeginAttributes() override
    {
        ThrowAttributesNotSupported();
    }

    virtual void OnMyEndAttributes() override
    {
        ThrowAttributesNotSupported();
    }


    void BeginNestedMessage()
    {
        auto index =  static_cast<int>(NestedMessages_.size());
        NestedMessages_.emplace_back(BodyCodedStream_.ByteCount(), -1);
        NestedIndexStack_.push_back(index);
    }

    void EndNestedMessage()
    {
        int index = NestedIndexStack_.back();
        NestedIndexStack_.pop_back();
        Y_ASSERT(NestedMessages_[index].Hi == -1);
        NestedMessages_[index].Hi = BodyCodedStream_.ByteCount();
    }

    void Finish()
    {
        YCHECK(YPathStack_.IsEmpty());
        YCHECK(!FieldStack_.back().Field);

        BodyCodedStream_.Trim();

        int bodyLength = static_cast<int>(BodyString_.length());
        NestedMessages_.emplace_back(bodyLength, std::numeric_limits<int>::max());

        {
            int nestedIndex = 0;
            std::function<int(int, int)> computeByteSize = [&] (int lo, int hi) {
                auto position = lo;
                int result = 0;
                while (true) {
                    auto& nestedMessage = NestedMessages_[nestedIndex];

                    {
                        auto threshold = std::min(hi, nestedMessage.Lo);
                        result += (threshold - position);
                        position = threshold;
                    }

                    if (nestedMessage.Lo == position && nestedMessage.Hi < std::numeric_limits<int>::max()) {
                        ++nestedIndex;
                        int nestedResult = computeByteSize(nestedMessage.Lo, nestedMessage.Hi);
                        nestedMessage.ByteSize = nestedResult;
                        result += BodyCodedStream_.VarintSize32(static_cast<ui32>(nestedResult));
                        result += nestedResult;
                        position = nestedMessage.Hi;
                    } else {
                        break;
                    }
                }
                return result;
            };
            computeByteSize(0, bodyLength);
        }

        {
            int nestedIndex = 0;
            std::function<void(int, int)> write = [&] (int lo, int hi) {
                auto position = lo;
                while (true) {
                    const auto& nestedMessage = NestedMessages_[nestedIndex];

                    {
                        auto threshold = std::min(hi, nestedMessage.Lo);
                        if (threshold > position) {
                            WriteRaw(BodyString_.data() + position, threshold - position);
                        }
                        position = threshold;
                    }

                    if (nestedMessage.Lo == position && nestedMessage.Hi < std::numeric_limits<int>::max()) {
                        ++nestedIndex;
                        char buf[16];
                        auto length = WriteVarUint64(buf, nestedMessage.ByteSize);
                        WriteRaw(buf, length);
                        write(nestedMessage.Lo, nestedMessage.Hi);
                        position = nestedMessage.Hi;
                    } else {
                        break;
                    }
                }
            };
            write(0, bodyLength);
        }
    }

    void WriteRaw(const char* data, int size)
    {
        while (true) {
            void* chunkData;
            int chunkSize;
            if (!OutputStream_->Next(&chunkData, &chunkSize)) {
                THROW_ERROR_EXCEPTION("Error writing to output stream");
            }
            auto bytesToWrite = std::min(chunkSize, size);
            ::memcpy(chunkData, data, bytesToWrite);
            if (bytesToWrite == size) {
                OutputStream_->BackUp(chunkSize - size);
                break;
            }
            data += bytesToWrite;
            size -= bytesToWrite;
        }
    }


    void ValidateNotRoot()
    {
        if (FieldStack_.empty()) {
            THROW_ERROR_EXCEPTION("Protobuf message can only be parsed from \"map\" values")
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("protobuf_type", RootType_->GetFullName());
        }
    }

    void ValidateNotRepeated()
    {
        if (FieldStack_.back().InList) {
            return;
        }
        const auto* field = FieldStack_.back().Field;
        if (field->IsYsonMap()) {
            THROW_ERROR_EXCEPTION("Map %v cannot be parsed from scalar values",
                YPathStack_.GetPath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("protobuf_field", field->GetFullName());
        }
        if (field->IsRepeated()) {
            THROW_ERROR_EXCEPTION("Field %v is repeated and cannot be parsed from scalar values",
                YPathStack_.GetPath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("protobuf_field", field->GetFullName());
        }
    }

    void ValidateRepeated()
    {
        if (FieldStack_.back().InList) {
            THROW_ERROR_EXCEPTION("Items of list %v cannot be lists themselves",
                YPathStack_.GetPath())
                << TErrorAttribute("ypath", YPathStack_.GetPath());
        }

        const auto* field = FieldStack_.back().Field;
        if (!field->IsRepeated()) {
            THROW_ERROR_EXCEPTION("Field %v is not repeated and cannot be parsed from \"list\" values",
                YPathStack_.GetPath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("protobuf_field", field->GetFullName());
        }
    }

    void WriteTag()
    {
        Y_ASSERT(!FieldStack_.empty());
        const auto* field = FieldStack_.back().Field;
        BodyCodedStream_.WriteTag(field->GetTag());
    }

    template <class F>
    void WriteScalar(F func)
    {
        ValidateNotRoot();
        ValidateNotRepeated();
        WriteTag();
        func();
        FieldStack_.pop_back();
        YPathStack_.Pop();
    }


    template <class T>
    void OnIntegerScalar(T value)
    {
        WriteScalar([&] {
            const auto* field = FieldStack_.back().Field;
            switch (field->GetType()) {
                case FieldDescriptor::TYPE_INT32: {
                    auto i32Value = CheckedCast<i32>(value, AsStringBuf("i32"));
                    BodyCodedStream_.WriteVarint32SignExtended(i32Value);
                    break;
                }

                case FieldDescriptor::TYPE_INT64: {
                    auto i64Value = CheckedCast<i64>(value, AsStringBuf("i64"));
                    BodyCodedStream_.WriteVarint64(static_cast<ui64>(i64Value));
                    break;
                }

                case FieldDescriptor::TYPE_SINT32: {
                    auto i32Value = CheckedCast<i32>(value, AsStringBuf("i32"));
                    BodyCodedStream_.WriteVarint64(ZigZagEncode64(i32Value));
                    break;
                }

                case FieldDescriptor::TYPE_SINT64: {
                    auto i64Value = CheckedCast<i64>(value, AsStringBuf("i64"));
                    BodyCodedStream_.WriteVarint64(ZigZagEncode64(i64Value));
                    break;
                }

                case FieldDescriptor::TYPE_UINT32: {
                    auto ui32Value = CheckedCast<ui32>(value, AsStringBuf("ui32"));
                    BodyCodedStream_.WriteVarint32(ui32Value);
                    break;
                }

                case FieldDescriptor::TYPE_UINT64: {
                    auto ui64Value = CheckedCast<ui64>(value, AsStringBuf("ui64"));
                    BodyCodedStream_.WriteVarint64(ui64Value);
                    break;
                }

                case FieldDescriptor::TYPE_FIXED32: {
                    auto ui32Value = CheckedCast<ui32>(value, AsStringBuf("ui32"));
                    BodyCodedStream_.WriteRaw(&ui32Value, sizeof(ui32Value));
                    break;
                }

                case FieldDescriptor::TYPE_FIXED64: {
                    auto ui64Value = CheckedCast<ui64>(value, AsStringBuf("ui64"));
                    BodyCodedStream_.WriteRaw(&ui64Value, sizeof(ui64Value));
                    break;
                }

                case FieldDescriptor::TYPE_ENUM: {
                    auto i32Value = CheckedCast<i32>(value, AsStringBuf("i32"));
                    const auto* enumType = field->GetEnumType();
                    auto literal = enumType->FindLiteralByValue(i32Value);
                    if (!literal) {
                        THROW_ERROR_EXCEPTION("Unknown value %v for field %v",
                            i32Value,
                            YPathStack_.GetPath())
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_field", field->GetFullName());
                    }
                    BodyCodedStream_.WriteVarint32SignExtended(i32Value);
                    break;
                }

                default:
                    THROW_ERROR_EXCEPTION("Field %v cannot be parsed from integer values",
                        YPathStack_.GetPath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath())
                        << TErrorAttribute("proto_field", field->GetFullName());
            }
        });
    }

    template <class TTo, class TFrom>
    TTo CheckedCast(TFrom value, TStringBuf toTypeName)
    {
        const auto* field = FieldStack_.back().Field;
        TTo result;
        if (!TryIntegralCast<TTo>(value, &result)) {
            THROW_ERROR_EXCEPTION("Value %v of field %v cannot fit into %Qv",
                value,
                YPathStack_.GetPath(),
                toTypeName)
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("protobuf_field", field->GetFullName());
        }
        return result;
    }
};

std::unique_ptr<IYsonConsumer> CreateProtobufWriter(
    ZeroCopyOutputStream* outputStream,
    const TProtobufMessageType* rootType,
    const TProtobufWriterOptions& options)
{
    return std::make_unique<TProtobufWriter>(outputStream, rootType, options);
}

////////////////////////////////////////////////////////////////////////////////

class TProtobufParser
    : public TProtobufTranscoderBase
{
public:
    TProtobufParser(
        IYsonConsumer* consumer,
        ZeroCopyInputStream* inputStream,
        const TProtobufMessageType* rootType,
        const TProtobufParserOptions& options)
        : Consumer_(consumer)
        , RootType_(rootType)
        , Options_(options)
        , InputStream_(inputStream)
        , CodedStream_(InputStream_)
    { }

    void Parse()
    {
        TypeStack_.emplace_back(RootType_);
        Consumer_->OnBeginMap();

        while (true) {
            auto& typeEntry = TypeStack_.back();
            const auto* type = typeEntry.Type;

            bool flag;
            if (type->IsAttributeDictionary()) {
                flag = ParseAttributeDictionary();
            } else if (IsYsonMapEntry()) {
                flag = ParseMapEntry();
            } else {
                flag = ParseRegular();
            }

            if (!flag) {
                if (typeEntry.RepeatedField) {
                    if (typeEntry.RepeatedField->IsYsonMap()) {
                        OnEndMap();
                    } else {
                        OnEndList();
                    }
                }

                SortFields(&typeEntry.OptionalFieldNumbers);
                ValidateNoFieldDuplicates(type, typeEntry.OptionalFieldNumbers);

                SortFields(&typeEntry.RequiredFieldNumbers);
                ValidateNoFieldDuplicates(type, typeEntry.RequiredFieldNumbers);

                if (!Options_.SkipRequiredFields && !IsYsonMapEntry()) {
                    ValidateRequiredFieldsPresent(type, typeEntry.RequiredFieldNumbers);
                }

                if (TypeStack_.size() == 1) {
                    break;
                }

                if (IsYsonMapEntry()) {
                    if (typeEntry.RequiredFieldNumbers.size() != 2) {
                        THROW_ERROR_EXCEPTION("Incomplete entry in protobuf map")
                            << TErrorAttribute("ypath", YPathStack_.GetPath());
                    }
                } else {
                    OnEndMap();
                }
                TypeStack_.pop_back();

                CodedStream_.PopLimit(LimitStack_.back());
                LimitStack_.pop_back();
                continue;
            }
        }

        Consumer_->OnEndMap();
        TypeStack_.pop_back();

        YCHECK(TypeStack_.empty());
        YCHECK(YPathStack_.IsEmpty());
        YCHECK(LimitStack_.empty());
    }

private:
    IYsonConsumer* const Consumer_;
    const TProtobufMessageType* const RootType_;
    const TProtobufParserOptions Options_;
    ZeroCopyInputStream* const InputStream_;

    CodedInputStream CodedStream_;

    struct TTypeEntry
    {
        explicit TTypeEntry(const TProtobufMessageType* type)
            : Type(type)
        { }

        const TProtobufMessageType* Type;
        TFieldNumberList RequiredFieldNumbers;
        TFieldNumberList OptionalFieldNumbers;
        const TProtobufField* RepeatedField = nullptr;
        int RepeatedIndex = -1;

        void BeginRepeated(const TProtobufField* field)
        {
            Y_ASSERT(!RepeatedField);
            Y_ASSERT(RepeatedIndex == -1);
            RepeatedField = field;
            RepeatedIndex = 0;
        }

        void ResetRepeated()
        {
            RepeatedField = nullptr;
            RepeatedIndex = -1;
        }

        int GenerateNextListIndex()
        {
            Y_ASSERT(RepeatedField);
            return ++RepeatedIndex;
        }
    };
    std::vector<TTypeEntry> TypeStack_;

    std::vector<CodedInputStream::Limit> LimitStack_;

    std::vector<char> PooledString_;
    std::vector<char> PooledAttributeKey_;
    std::vector<char> PooledAttributeValue_;


    void OnBeginMap()
    {
        Consumer_->OnBeginMap();
    }

    void OnKeyedItem(const TProtobufField* field)
    {
        Consumer_->OnKeyedItem(field->GetYsonName());
        YPathStack_.Push(field);
    }

    void OnKeyedItem(TString key)
    {
        Consumer_->OnKeyedItem(key);
        YPathStack_.Push(std::move(key));
    }

    void OnEndMap()
    {
        Consumer_->OnEndMap();
        YPathStack_.Pop();
    }


    void OnBeginList()
    {
        Consumer_->OnBeginList();
    }

    void OnListItem(int index)
    {
        Consumer_->OnListItem();
        YPathStack_.Push(index);
    }

    void OnEndList()
    {
        Consumer_->OnEndList();
        YPathStack_.Pop();
    }


    bool IsYsonMapEntry()
    {
        if (TypeStack_.size() < 2) {
            return false;
        }
        auto& typeEntry = TypeStack_[TypeStack_.size() - 2];
        if (!typeEntry.RepeatedField) {
            return false;
        }
        if (!typeEntry.RepeatedField->IsYsonMap()) {
            return false;
        }
        return true;
    }

    bool ParseMapEntry()
    {
        auto& typeEntry = TypeStack_.back();
        const auto* type = typeEntry.Type;

        auto tag = CodedStream_.ReadTag();
        if (tag == 0) {
            return false;
        }

        auto wireType = WireFormatLite::GetTagWireType(tag);
        auto fieldNumber = WireFormatLite::GetTagFieldNumber(tag);
        typeEntry.RequiredFieldNumbers.push_back(fieldNumber);

        switch (fieldNumber) {
            case ProtobufMapKeyFieldNumber: {
                if (typeEntry.RequiredFieldNumbers.size() != 1) {
                    THROW_ERROR_EXCEPTION("Out-of-order protobuf map key")
                        << TErrorAttribute("ypath", YPathStack_.GetPath());
                }

                if (wireType != WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
                    THROW_ERROR_EXCEPTION("Unexpected wire type tag %x for protobuf map key",
                        tag)
                        << TErrorAttribute("ypath", YPathStack_.GetPath());
                }

                ui64 length;
                if (!CodedStream_.ReadVarint64(&length)) {
                    THROW_ERROR_EXCEPTION("Error reading \"varint\" value for protobuf map key")
                        << TErrorAttribute("ypath", YPathStack_.GetPath());
                }

                PooledString_.resize(length);
                if (!CodedStream_.ReadRaw(PooledString_.data(), length)) {
                    THROW_ERROR_EXCEPTION("Error reading \"string\" value for protobuf map key")
                        << TErrorAttribute("ypath", YPathStack_.GetPath());
                }

                OnKeyedItem(TString(PooledString_.data(), length));
                break;
            }

            case ProtobufMapValueFieldNumber: {
                if (typeEntry.RequiredFieldNumbers.size() != 2) {
                    THROW_ERROR_EXCEPTION("Out-of-order protobuf map value")
                        << TErrorAttribute("ypath", YPathStack_.GetPath());
                }

                const auto* field = type->GetFieldByNumber(fieldNumber);
                ParseFieldValue(field, tag, wireType);
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Unexpected field number %v in protobuf map",
                    fieldNumber)
                    << TErrorAttribute("ypath", YPathStack_.GetPath());
        }

        return true;
    }

    bool ParseRegular()
    {
        auto& typeEntry = TypeStack_.back();
        const auto* type = typeEntry.Type;

        auto tag = CodedStream_.ReadTag();
        if (tag == 0) {
            return false;
        }

        auto wireType = WireFormatLite::GetTagWireType(tag);
        auto fieldNumber = WireFormatLite::GetTagFieldNumber(tag);
        const auto* field = type->FindFieldByNumber(fieldNumber);
        if (!field) {
            if (Options_.SkipUnknownFields) {
                switch (wireType) {
                    case WireFormatLite::WIRETYPE_VARINT: {
                        ui64 unsignedValue;
                        if (!CodedStream_.ReadVarint64(&unsignedValue)) {
                            THROW_ERROR_EXCEPTION("Error reading \"varint\" value for unknown field %v",
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        break;
                    }

                    case WireFormatLite::WIRETYPE_FIXED32: {
                        ui32 unsignedValue;
                        if (!CodedStream_.ReadLittleEndian32(&unsignedValue)) {
                            THROW_ERROR_EXCEPTION("Error reading \"fixed32\" value for field %v",
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        break;
                    }

                    case WireFormatLite::WIRETYPE_FIXED64: {
                        ui64 unsignedValue;
                        if (!CodedStream_.ReadLittleEndian64(&unsignedValue)) {
                            THROW_ERROR_EXCEPTION("Error reading \"fixed64\" value for unknown field %v",
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        break;
                    }

                    case WireFormatLite::WIRETYPE_LENGTH_DELIMITED: {
                        ui64 length;
                        if (!CodedStream_.ReadVarint64(&length)) {
                            THROW_ERROR_EXCEPTION("Error reading \"varint\" value for unknown field %v",
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        if (length > std::numeric_limits<int>::max()) {
                            THROW_ERROR_EXCEPTION("Invalid length %v for unknown field %v",
                                length,
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        if (!CodedStream_.Skip(static_cast<int>(length))) {
                            THROW_ERROR_EXCEPTION("Error skipping unknown length-delimited field %v",
                                fieldNumber)
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        break;
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Unexpected wire type tag %x for unknown field %v",
                            tag,
                            fieldNumber)
                            << TErrorAttribute("ypath", YPathStack_.GetPath());
                }
                return true;
            }
            THROW_ERROR_EXCEPTION("Unknown field number %v at %v",
                fieldNumber,
                YPathStack_.GetPath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_type", type->GetFullName());
        }

        if (typeEntry.RepeatedField == field) {
            Y_ASSERT(field->IsRepeated());
            if (!field->IsYsonMap()) {
                OnListItem(typeEntry.GenerateNextListIndex());
            }
        } else {
            if (typeEntry.RepeatedField) {
                if (typeEntry.RepeatedField->IsYsonMap()) {
                    Consumer_->OnEndMap();
                } else {
                    Consumer_->OnEndList();
                }
                YPathStack_.Pop();
            }
            typeEntry.ResetRepeated();

            OnKeyedItem(field);

            if (field->IsYsonMap()) {
                typeEntry.BeginRepeated(field);
                OnBeginMap();
            } else if (field->IsRepeated()) {
                typeEntry.BeginRepeated(field);
                OnBeginList();
                OnListItem(0);
            }
        }

        if (field->IsRequired()) {
            typeEntry.RequiredFieldNumbers.push_back(fieldNumber);
        } else if (field->IsOptional()) {
            typeEntry.OptionalFieldNumbers.push_back(fieldNumber);
        }

        ParseFieldValue(field, tag, wireType);

        return true;
    }

    void ParseFieldValue(
        const TProtobufField* field,
        int tag,
        WireFormatLite::WireType wireType)
    {
        switch (wireType) {
            case WireFormatLite::WIRETYPE_VARINT: {
                ui64 unsignedValue;
                if (!CodedStream_.ReadVarint64(&unsignedValue)) {
                    THROW_ERROR_EXCEPTION("Error reading \"varint\" value for field %v",
                        YPathStack_.GetPath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath());
                }

                switch (field->GetType()) {
                    case FieldDescriptor::TYPE_BOOL:
                        ParseScalar([&] {
                            Consumer_->OnBooleanScalar(unsignedValue != 0);
                        });
                        break;

                    case FieldDescriptor::TYPE_ENUM: {
                        auto signedValue = static_cast<int>(unsignedValue);
                        const auto* enumType = field->GetEnumType();
                        auto literal = enumType->FindLiteralByValue(signedValue);
                        if (!literal) {
                            THROW_ERROR_EXCEPTION("Unknown value %v for field %v",
                                signedValue,
                                YPathStack_.GetPath())
                                << TErrorAttribute("ypath", YPathStack_.GetPath())
                                << TErrorAttribute("proto_field", field->GetFullName());
                        }
                        ParseScalar([&] {
                            Consumer_->OnStringScalar(literal);
                        });
                        break;
                    }

                    case FieldDescriptor::TYPE_INT32:
                    case FieldDescriptor::TYPE_INT64:
                        ParseScalar([&] {
                            auto signedValue = static_cast<i64>(unsignedValue);
                            Consumer_->OnInt64Scalar(signedValue);
                        });
                        break;

                    case FieldDescriptor::TYPE_UINT32:
                    case FieldDescriptor::TYPE_UINT64:
                        ParseScalar([&] {
                            Consumer_->OnUint64Scalar(unsignedValue);
                        });
                        break;

                    case FieldDescriptor::TYPE_SINT64:
                    case FieldDescriptor::TYPE_SINT32:
                        ParseScalar([&] {
                            auto signedValue = ZigZagDecode64(unsignedValue);
                            Consumer_->OnInt64Scalar(signedValue);
                        });
                        break;

                    default:
                        THROW_ERROR_EXCEPTION("Unexpected \"varint\" value for field %v",
                            YPathStack_.GetPath())
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_field", field->GetFullName());
                }
                break;
            }

            case WireFormatLite::WIRETYPE_FIXED32: {
                ui32 unsignedValue;
                if (!CodedStream_.ReadLittleEndian32(&unsignedValue)) {
                    THROW_ERROR_EXCEPTION("Error reading \"fixed32\" value for field %v",
                        YPathStack_.GetPath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath());
                }

                switch (field->GetType()) {
                    case FieldDescriptor::TYPE_FIXED32:
                        ParseScalar([&] {
                            Consumer_->OnUint64Scalar(unsignedValue);
                        });
                        break;

                    case FieldDescriptor::TYPE_SFIXED32: {
                        ParseScalar([&] {
                            auto signedValue = static_cast<i32>(unsignedValue);
                            Consumer_->OnInt64Scalar(signedValue);
                        });
                        break;
                    }

                    case FieldDescriptor::TYPE_FLOAT: {
                        ParseScalar([&] {
                            auto floatValue = WireFormatLite::DecodeFloat(unsignedValue);
                            Consumer_->OnDoubleScalar(floatValue);
                        });
                        break;
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Unexpected \"fixed32\" value for field %v",
                            YPathStack_.GetPath())
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_field", field->GetFullName());
                }
                break;
            }

            case WireFormatLite::WIRETYPE_FIXED64: {
                ui64 unsignedValue;
                if (!CodedStream_.ReadLittleEndian64(&unsignedValue)) {
                    THROW_ERROR_EXCEPTION("Error reading \"fixed64\" value for field %v",
                        YPathStack_.GetPath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath());
                }

                switch (field->GetType()) {
                    case FieldDescriptor::TYPE_FIXED64:
                        ParseScalar([&] {
                            Consumer_->OnUint64Scalar(unsignedValue);
                        });
                        break;

                    case FieldDescriptor::TYPE_SFIXED64: {
                        ParseScalar([&] {
                            auto signedValue = static_cast<i64>(unsignedValue);
                            Consumer_->OnInt64Scalar(signedValue);
                        });
                        break;
                    }

                    case FieldDescriptor::TYPE_DOUBLE: {
                        ParseScalar([&] {
                            auto doubleValue = WireFormatLite::DecodeDouble(unsignedValue);
                            Consumer_->OnDoubleScalar(doubleValue);
                        });
                        break;
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Unexpected \"fixed64\" value for field %v",
                            YPathStack_.GetPath())
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_field", field->GetFullName());
                }
                break;
            }

            case WireFormatLite::WIRETYPE_LENGTH_DELIMITED: {
                ui64 length;
                if (!CodedStream_.ReadVarint64(&length)) {
                    THROW_ERROR_EXCEPTION("Error reading \"varint\" value for field %v",
                        YPathStack_.GetPath())
                        << TErrorAttribute("ypath", YPathStack_.GetPath());
                }

                switch (field->GetType()) {
                    case FieldDescriptor::TYPE_BYTES:
                    case FieldDescriptor::TYPE_STRING: {
                        PooledString_.resize(length);
                        if (!CodedStream_.ReadRaw(PooledString_.data(), length)) {
                            THROW_ERROR_EXCEPTION("Error reading \"string\" value for field %v",
                                YPathStack_.GetPath())
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        ParseScalar([&] {
                            if (field->IsYsonString()) {
                                Consumer_->OnRaw(TStringBuf(PooledString_.data(), length), NYson::EYsonType::Node);
                            } else {
                                Consumer_->OnStringScalar(TStringBuf(PooledString_.data(), length));
                            }
                        });
                        break;
                    }

                    case FieldDescriptor::TYPE_MESSAGE: {
                        LimitStack_.push_back(CodedStream_.PushLimit(static_cast<int>(length)));
                        TypeStack_.emplace_back(field->GetMessageType());
                        if (!IsYsonMapEntry()) {
                            OnBeginMap();
                        }
                        break;
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Unexpected \"length-delimited\" value for field %v",
                            YPathStack_.GetPath())
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_field", field->GetFullName());
                }
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Unexpected wire type tag %x",
                    tag)
                    << TErrorAttribute("ypath", YPathStack_.GetPath());
        }
    }

    bool ParseAttributeDictionary()
    {
        // XXX
        auto throwUnexpectedWireType = [&] (WireFormatLite::WireType actualWireType) {
            THROW_ERROR_EXCEPTION("Invalid wire type %v while parsing attribute dictionary %v",
                static_cast<int>(actualWireType),
                YPathStack_.GetPath())
                << TErrorAttribute("ypath", YPathStack_.GetPath());
        };

        auto expectWireType = [&] (WireFormatLite::WireType actualWireType, WireFormatLite::WireType expectedWireType) {
            if (actualWireType != expectedWireType) {
                throwUnexpectedWireType(actualWireType);
            }
        };

        auto throwUnexpectedFieldNumber = [&] (int actualFieldNumber) {
            THROW_ERROR_EXCEPTION("Invalid field number %v while parsing attribute dictionary %v",
                actualFieldNumber,
                YPathStack_.GetPath())
                << TErrorAttribute("ypath", YPathStack_.GetPath());
        };

        auto expectFieldNumber = [&] (int actualFieldNumber, int expectedFieldNumber) {
            if (actualFieldNumber != expectedFieldNumber) {
                throwUnexpectedFieldNumber(actualFieldNumber);
            }
        };

        auto readVarint64 = [&] () {
            ui64 value;
            if (!CodedStream_.ReadVarint64(&value)) {
                THROW_ERROR_EXCEPTION("Error reading \"varint\" value while parsing attribute dictionary %v",
                    YPathStack_.GetPath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath());
            }
            return value;
        };

        auto readString = [&] (auto* pool) -> TStringBuf {
            auto length = readVarint64();
            pool->resize(length);
            if (!CodedStream_.ReadRaw(pool->data(), length)) {
                THROW_ERROR_EXCEPTION("Error reading \"string\" value while parsing attribute dictionary %v",
                    YPathStack_.GetPath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath());
            }
            return TStringBuf(pool->data(), length);
        };

        while (true) {
            auto tag = CodedStream_.ReadTag();
            if (tag == 0) {
                return false;
            }

            expectWireType(WireFormatLite::GetTagWireType(tag), WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
            expectFieldNumber(WireFormatLite::GetTagFieldNumber(tag), 1);

            auto entryLength = readVarint64();
            LimitStack_.push_back(CodedStream_.PushLimit(static_cast<int>(entryLength)));

            std::optional<TStringBuf> key;
            std::optional<TStringBuf> value;
            while (true) {
                auto tag = CodedStream_.ReadTag();
                if (tag == 0) {
                    break;
                }

                auto fieldNumber = WireFormatLite::GetTagFieldNumber(tag);
                switch (fieldNumber) {
                    case AttributeDictionaryKeyFieldNumber: {
                        if (key) {
                            THROW_ERROR_EXCEPTION("Duplicate key found while parsing attribute dictionary %v",
                                YPathStack_.GetPath())
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        key = readString(&PooledAttributeKey_);
                        break;
                    }

                    case AttributeDictionaryValueFieldNumber: {
                        if (value) {
                            THROW_ERROR_EXCEPTION("Duplicate value found while parsing attribute dictionary %v",
                                YPathStack_.GetPath())
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        value = readString(&PooledAttributeValue_);
                        break;
                    }

                    default:
                        throwUnexpectedFieldNumber(fieldNumber);
                        break;
                }
            }

            if (!key) {
                THROW_ERROR_EXCEPTION("Missing key while parsing attribute dictionary %v",
                    YPathStack_.GetPath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath());
            }
            if (!value) {
                THROW_ERROR_EXCEPTION("Missing value while parsing attribute dictionary %v",
                    YPathStack_.GetPath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath());
            }

            Consumer_->OnKeyedItem(*key);
            Consumer_->OnRaw(*value, NYson::EYsonType::Node);

            CodedStream_.PopLimit(LimitStack_.back());
            LimitStack_.pop_back();
        }
    }

    template <class F>
    void ParseScalar(F func)
    {
        func();
        YPathStack_.Pop();
    }
};

void ParseProtobuf(
    IYsonConsumer* consumer,
    ZeroCopyInputStream* inputStream,
    const TProtobufMessageType* rootType,
    const TProtobufParserOptions& options)
{
    TProtobufParser parser(consumer, inputStream, rootType, options);
    parser.Parse();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
