#include "protobuf_interop.h"

#include <yt/core/yson/proto/protobuf_interop.pb.h>

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/writer.h>
#include <yt/core/yson/forwarding_consumer.h>

#include <yt/core/ypath/token.h>

#include <yt/core/misc/zigzag.h>
#include <yt/core/misc/varint.h>
#include <yt/core/misc/variant.h>
#include <yt/core/misc/cast.h>

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

////////////////////////////////////////////////////////////////////////////////

class TProtobufTypeRegistry
{
public:
    TStringBuf InternString(const TString& str)
    {
        auto guard = Guard(SpinLock_);
        InternedStrings_.push_back(str);
        return InternedStrings_.back();
    }

    TStringBuf GetYsonName(const FieldDescriptor* descriptor)
    {
        const auto& name = descriptor->options().GetExtension(NYT::NYson::NProto::field_name);
        if (name) {
            return InternString(name);
        } else {
            return InternString(descriptor->name());
        }
    }

    TStringBuf GetYsonLiteral(const EnumValueDescriptor* descriptor)
    {
        const auto& name = descriptor->options().GetExtension(NYT::NYson::NProto::enum_value_name);
        if (name) {
            return InternString(name);
        } else {
            return InternString(CamelCaseToUnderscoreCase(descriptor->name()));
        }
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
        , IsYsonString_(descriptor->options().GetExtension(NYT::NYson::NProto::yson_string))
    { }

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
        return IsYsonString_;
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
    const bool IsYsonString_;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufMessageType
{
public:
    TProtobufMessageType(TProtobufTypeRegistry* registry, const Descriptor* descriptor)
        : Registry_(registry)
        , Underlying_(descriptor)
        , IsAttributeDictionary_(descriptor->options().GetExtension(NYT::NYson::NProto::attribute_dictionary))
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
        return IsAttributeDictionary_;
    }

    const TString& GetFullName() const
    {
        return Underlying_->full_name();
    }

    const std::vector<int>& GetRequiredFieldNumbers() const
    {
        return RequiredFieldNumbers_;
    }

    const TProtobufField* FindFieldByName(const TStringBuf& name) const
    {
        auto it = NameToField_.find(name);
        return it == NameToField_.end() ? nullptr : it->second.get();
    }

    const TProtobufField* FindFieldByNumber(int number) const
    {
        auto it = NumberToField_.find(number);
        return it == NumberToField_.end() ? nullptr : it->second;
    }

private:
    TProtobufTypeRegistry* const Registry_;
    const Descriptor* const Underlying_;
    const bool IsAttributeDictionary_;

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
            YCHECK(LitrealToValue_.emplace(literal, valueDescriptor->number()).second);
            YCHECK(ValueToLiteral_.emplace(valueDescriptor->number(), literal).second);
        }
    }

    const TString& GetFullName() const
    {
        return Underlying_->full_name();
    }

    TNullable<int> FindValueByLiteral(const TStringBuf& literal) const
    {
        auto it = LitrealToValue_.find(literal);
        return it == LitrealToValue_.end() ? Null : MakeNullable(it->second);
    }

    TStringBuf FindLiteralByValue(int value) const
    {
        auto it = ValueToLiteral_.find(value);
        return it == ValueToLiteral_.end() ? TStringBuf() : it->second;
    }

private:
    TProtobufTypeRegistry* const Registry_;
    const EnumDescriptor* const Underlying_;

    THashMap<TStringBuf, int> LitrealToValue_;
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
    void Push(const TStringBuf& literal)
    {
        Items_.push_back(literal);
    }

    void Push(int index)
    {
        Items_.push_back(index);
    }

    void Pop()
    {
        Items_.pop_back();
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
                case TEntry::TagOf<TStringBuf>():
                    builder.AppendString(ToYPathLiteral(item.As<TStringBuf>()));
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
    using TEntry = TVariant<TStringBuf, int>;
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
                YPathStack_.Push(field->GetYsonName());
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
                const auto* field = type->FindFieldByNumber(numbers[index]);
                YCHECK(field);
                YPathStack_.Push(field->GetYsonName());
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
    TProtobufWriter(ZeroCopyOutputStream* outputStream, const TProtobufMessageType* rootType)
        : OutputStream_(outputStream)
        , RootType_(rootType)
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

    virtual void OnMyStringScalar(const TStringBuf& value) override
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
                    auto maybeValue = enumType->FindValueByLiteral(value);
                    if (!maybeValue) {
                        THROW_ERROR_EXCEPTION("Field %v cannot have value %Qv",
                            YPathStack_.GetPath(),
                            value)
                            << TErrorAttribute("ypath", YPathStack_.GetPath())
                            << TErrorAttribute("proto_type", enumType->GetFullName());
                    }
                    BodyCodedStream_.WriteVarint32SignExtended(*maybeValue);
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
        THROW_ERROR_EXCEPTION("Entities are not supported")
            << TErrorAttribute("ypath", YPathStack_.GetPath());
    }

    virtual void OnMyBeginList() override
    {
        ValidateNotRoot();
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
    }

    virtual void OnMyBeginMap() override
    {
        if (TypeStack_.empty()) {
            TypeStack_.emplace_back(RootType_);
            return;
        }

        const auto* field = FieldStack_.back().Field;
        if (field->GetType() != FieldDescriptor::TYPE_MESSAGE) {
            THROW_ERROR_EXCEPTION("Field %v cannot be parsed from \"map\" values",
                YPathStack_.GetPath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_field", field->GetFullName());
        }

        ValidateNotRepeated();
        TypeStack_.emplace_back(field->GetMessageType());
        WriteTag();
        int nestedIndex = BeginNestedMessage();
        NestedIndexStack_.push_back(nestedIndex);
    }

    virtual void OnMyKeyedItem(const TStringBuf& key) override
    {
        Y_ASSERT(TypeStack_.size() > 0);
        const auto* type = TypeStack_.back().Type;

        if (type->IsAttributeDictionary()) {
            OnMyKeyedItemAttributeDictionary(key);
        } else {
            OnMyKeyedItemRegular(key);
        }
    }

    void OnMyKeyedItemRegular(const TStringBuf& key)
    {
        const auto* type = TypeStack_.back().Type;
        const auto* field = type->FindFieldByName(key);
        if (!field) {
            THROW_ERROR_EXCEPTION("Unknown field %Qv at %v",
                key,
                YPathStack_.GetPath())
                << TErrorAttribute("ypath", YPathStack_.GetPath())
                << TErrorAttribute("proto_type", type->GetFullName());
        }

        auto number = field->GetNumber();
        if (field->IsRequired()) {
            TypeStack_.back().RequiredFieldNumbers.push_back(number);
        } else {
            TypeStack_.back().NonRequiredFieldNumbers.push_back(number);
        }

        FieldStack_.emplace_back(field, 0, false);
        YPathStack_.Push(field->GetYsonName());

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

    void OnMyKeyedItemAttributeDictionary(const TStringBuf& key)
    {
        AttributeKey_ = key;
        AttributeValue_.clear();
        Forward(&AttributeValueWriter_, [this] {
            AttributeValueWriter_.Flush();

            BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(1 /*attribute*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
            BodyCodedStream_.WriteVarint64(
                1 +
                CodedOutputStream::VarintSize64(AttributeKey_.length()) +
                AttributeKey_.length() +
                1 +
                CodedOutputStream::VarintSize64(AttributeValue_.length()) +
                AttributeValue_.length());

            BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(1 /*key*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
            BodyCodedStream_.WriteVarint64(AttributeKey_.length());
            BodyCodedStream_.WriteRaw(AttributeKey_.data(), AttributeKey_.length());

            BodyCodedStream_.WriteTag(google::protobuf::internal::WireFormatLite::MakeTag(2 /*value*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
            BodyCodedStream_.WriteVarint64(AttributeValue_.length());
            BodyCodedStream_.WriteRaw(AttributeValue_.data(), AttributeValue_.length());
        });
    }

    virtual void OnMyEndMap() override
    {
        auto& typeEntry = TypeStack_.back();
        auto* type = typeEntry.Type;

        SortFields(&typeEntry.NonRequiredFieldNumbers);
        ValidateNoFieldDuplicates(type, typeEntry.NonRequiredFieldNumbers);

        SortFields(&typeEntry.RequiredFieldNumbers);
        ValidateNoFieldDuplicates(type, typeEntry.RequiredFieldNumbers);
        ValidateRequiredFieldsPresent(type, typeEntry.RequiredFieldNumbers);

        TypeStack_.pop_back();
        if (TypeStack_.empty()) {
            Finish();
            return;
        }

        FieldStack_.pop_back();
        YPathStack_.Pop();
        int nestedIndex = NestedIndexStack_.back();
        NestedIndexStack_.pop_back();
        EndNestedMessage(nestedIndex);
    }

    virtual void OnMyBeginAttributes() override
    {
        THROW_ERROR_EXCEPTION("Attributes are not supported")
            << TErrorAttribute("ypath", YPathStack_.GetPath());
    }

    virtual void OnMyEndAttributes() override
    {
        THROW_ERROR_EXCEPTION("Attributes are not supported")
            << TErrorAttribute("ypath", YPathStack_.GetPath());
    }


    int BeginNestedMessage()
    {
        auto index =  static_cast<int>(NestedMessages_.size());
        NestedMessages_.emplace_back(BodyCodedStream_.ByteCount(), -1);
        return index;
    }

    void EndNestedMessage(int index)
    {
        Y_ASSERT(NestedMessages_[index].Hi == -1);
        NestedMessages_[index].Hi = BodyCodedStream_.ByteCount();
    }

    void Finish()
    {
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
                    auto i32Value = CheckedCast<i32>(value, STRINGBUF("i32"));
                    BodyCodedStream_.WriteVarint32SignExtended(i32Value);
                    break;
                }

                case FieldDescriptor::TYPE_INT64: {
                    auto i64Value = CheckedCast<i64>(value, STRINGBUF("i64"));
                    BodyCodedStream_.WriteVarint64(static_cast<ui64>(i64Value));
                    break;
                }

                case FieldDescriptor::TYPE_SINT32: {
                    auto i32Value = CheckedCast<i32>(value, STRINGBUF("i32"));
                    BodyCodedStream_.WriteVarint64(ZigZagEncode64(i32Value));
                    break;
                }

                case FieldDescriptor::TYPE_SINT64: {
                    auto i64Value = CheckedCast<i64>(value, STRINGBUF("i64"));
                    BodyCodedStream_.WriteVarint64(ZigZagEncode64(i64Value));
                    break;
                }

                case FieldDescriptor::TYPE_UINT32: {
                    auto ui32Value = CheckedCast<ui32>(value, STRINGBUF("ui32"));
                    BodyCodedStream_.WriteVarint32(ui32Value);
                    break;
                }

                case FieldDescriptor::TYPE_UINT64: {
                    auto ui64Value = CheckedCast<ui64>(value, STRINGBUF("ui64"));
                    BodyCodedStream_.WriteVarint64(ui64Value);
                    break;
                }

                case FieldDescriptor::TYPE_FIXED32: {
                    auto ui32Value = CheckedCast<ui32>(value, STRINGBUF("ui32"));
                    BodyCodedStream_.WriteRaw(&ui32Value, sizeof(ui32Value));
                    break;
                }

                case FieldDescriptor::TYPE_FIXED64: {
                    auto ui64Value = CheckedCast<ui64>(value, STRINGBUF("ui64"));
                    BodyCodedStream_.WriteRaw(&ui64Value, sizeof(ui64Value));
                    break;
                }

                case FieldDescriptor::TYPE_ENUM: {
                    auto i32Value = CheckedCast<i32>(value, STRINGBUF("i32"));
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
    TTo CheckedCast(TFrom value, const TStringBuf& toTypeName)
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
    const TProtobufMessageType* rootType)
{
    return std::make_unique<TProtobufWriter>(outputStream, rootType);
}

////////////////////////////////////////////////////////////////////////////////

class TProtobufParser
    : public TProtobufTranscoderBase
{
public:
    TProtobufParser(
        IYsonConsumer* consumer,
        ZeroCopyInputStream* inputStream,
        const TProtobufMessageType* rootType)
        : Consumer_(consumer)
        , RootType_(rootType)
        , InputStream_(inputStream)
        , CodedStream_(InputStream_)
    { }

    void Parse()
    {
        TypeStack_.emplace_back(RootType_);
        RepeatedFieldNumberStack_.emplace_back();

        Consumer_->OnBeginMap();

        while (true) {
            auto& typeEntry = TypeStack_.back();
            const auto* type = typeEntry.Type;

            bool flag;
            if (type->IsAttributeDictionary()) {
                flag = ParseAttributeDictionary();
            } else {
                flag = ParseRegular();
            }

            if (!flag) {
                if (RepeatedFieldNumberStack_.back().FieldNumber != -1) {
                    Consumer_->OnEndList();
                }
                RepeatedFieldNumberStack_.pop_back();

                SortFields(&typeEntry.OptionalFieldNumbers);
                ValidateNoFieldDuplicates(type, typeEntry.OptionalFieldNumbers);

                SortFields(&typeEntry.RequiredFieldNumbers);
                ValidateNoFieldDuplicates(type, typeEntry.RequiredFieldNumbers);
                ValidateRequiredFieldsPresent(type, typeEntry.RequiredFieldNumbers);

                Consumer_->OnEndMap();

                TypeStack_.pop_back();
                if (TypeStack_.empty()) {
                    break;
                }

                YPathStack_.Pop();
                CodedStream_.PopLimit(LimitStack_.back());
                LimitStack_.pop_back();
                continue;
            }
        }
    }

private:
    IYsonConsumer* const Consumer_;
    const TProtobufMessageType* const RootType_;
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
    };
    std::vector<TTypeEntry> TypeStack_;

    std::vector<CodedInputStream::Limit> LimitStack_;

    struct TRepeatedFieldEntry
    {
        explicit TRepeatedFieldEntry(int fieldNumber = -1, int listIndex = -1)
            : FieldNumber(fieldNumber)
            , ListIndex(listIndex)
        { }

        int FieldNumber = -1;
        int ListIndex = -1;
    };
    std::vector<TRepeatedFieldEntry> RepeatedFieldNumberStack_;

    std::vector<char> PooledString_;
    std::vector<char> PooledAttributeKey_;
    std::vector<char> PooledAttributeValue_;


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
            THROW_ERROR_EXCEPTION("Unknown field number %v at %v",
                fieldNumber,
                YPathStack_.GetPath())
                    << TErrorAttribute("ypath", YPathStack_.GetPath())
                    << TErrorAttribute("proto_type", type->GetFullName());
        }

        if (RepeatedFieldNumberStack_.back().FieldNumber == fieldNumber) {
            Y_ASSERT(field->IsRepeated());
            Consumer_->OnListItem();
            YPathStack_.Push(++RepeatedFieldNumberStack_.back().ListIndex);
        } else {
            if (RepeatedFieldNumberStack_.back().FieldNumber != -1) {
                Consumer_->OnEndList();
                RepeatedFieldNumberStack_.back() = TRepeatedFieldEntry();
                YPathStack_.Pop();
            }

            Consumer_->OnKeyedItem(field->GetYsonName());
            YPathStack_.Push(field->GetYsonName());

            if (field->IsRepeated()) {
                RepeatedFieldNumberStack_.back() = TRepeatedFieldEntry(fieldNumber, 0);
                Consumer_->OnBeginList();
                Consumer_->OnListItem();
                YPathStack_.Push(0);
            }
        }

        if (field->IsRequired()) {
            typeEntry.RequiredFieldNumbers.push_back(fieldNumber);
        } else if (field->IsOptional()) {
            typeEntry.OptionalFieldNumbers.push_back(fieldNumber);
        }

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
                        RepeatedFieldNumberStack_.emplace_back();
                        LimitStack_.push_back(CodedStream_.PushLimit(static_cast<int>(length)));
                        TypeStack_.emplace_back(field->GetMessageType());
                        Consumer_->OnBeginMap();
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

        return true;
    }

    bool ParseAttributeDictionary()
    {
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

            TNullable<TStringBuf> key;
            TNullable<TStringBuf> value;
            while (true) {
                auto tag = CodedStream_.ReadTag();
                if (tag == 0) {
                    break;
                }

                auto fieldNumber = WireFormatLite::GetTagFieldNumber(tag);
                switch (fieldNumber) {
                    case 1: {
                        // Key
                        if (key) {
                            THROW_ERROR_EXCEPTION("Duplicate key found while parsing attribute dictionary %v",
                                YPathStack_.GetPath())
                                << TErrorAttribute("ypath", YPathStack_.GetPath());
                        }
                        key = readString(&PooledAttributeKey_);
                        break;
                    }

                    case 2: {
                        // Value
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
    const TProtobufMessageType* rootType)
{
    TProtobufParser parser(consumer, inputStream, rootType);
    parser.Parse();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
