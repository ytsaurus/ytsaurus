#include "serializer.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/coding/varint.h>

#include <library/cpp/yt/yson/consumer.h>

#include <library/cpp/iterator/enumerate.h>

#include <array>
#include <memory>
#include <stack>

namespace NYT::NFlow::NYsonSerializer {

using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename TValue>
EValueType GetValueType();

#define XX(type, cppType) \
template <> \
EValueType GetValueType<cppType>() \
{ \
    return EValueType::type; \
}

XX(String, TStringBuf)
XX(Uint64, ui64)
XX(Int64, i64)
XX(Double, double)
XX(Boolean, bool)

#undef XX

const TLogicalType& Unwrap(const TLogicalType& type)
{
    if (type.GetMetatype() == ELogicalMetatype::Optional) {
        return *type.UncheckedAsOptionalTypeRef().GetElement();
    }
    return type;
}

template <class TValue>
void ValidateSimpleType(const TLogicalType& logicalType, TValue value)
{
    const auto simpleType = GetPhysicalType(Unwrap(logicalType).AsSimpleTypeRef().GetElement());

    if constexpr (std::same_as<TValue, TStringBuf>) {
        if (simpleType == GetValueType<i64>()) {
            THROW_ERROR_EXCEPTION_UNLESS(value.size() == 1,
                "Char value %Qv has more than 1 byte", value);
            return;
        }
    }

    const auto valueType = GetValueType<TValue>();
    THROW_ERROR_EXCEPTION_UNLESS(simpleType == valueType,
        "Expected type %Qv, but got %Qv", simpleType, valueType);
}

////////////////////////////////////////////////////////////////////////////////

class TStructDescriptor
{
public:
    TStructDescriptor() = default;

    explicit TStructDescriptor(const TStructLogicalType& structType)
        : Fields_(structType.GetFields())
    {
        for (const auto& [index, field] : Enumerate(Fields_)) {
            NameToFieldIndex_[field.Name] = index;
        }
    }

    int GetFieldIdOrThrow(TStringBuf name) const
    {
        return NameToFieldIndex_.at(name);
    }

    const TLogicalTypePtr& GetFieldTypeOrThrow(TStringBuf name) const
    {
        return GetFieldTypeOrThrow(GetFieldIdOrThrow(name));
    }

    const TLogicalTypePtr& GetFieldTypeOrThrow(int fieldId) const
    {
        return GetFieldOrThrow(fieldId).Type;
    }

    const std::string& GetFieldNameOrThrow(int fieldId) const
    {
        return GetFieldOrThrow(fieldId).Name;
    }

    const TStructField& GetFieldOrThrow(int fieldId) const
    {
        return Fields_.at(fieldId);
    }

    void Reset()
    {
        NameToFieldIndex_.clear();
        Fields_.clear();
    }

    int GetCount() const
    {
        return std::ssize(Fields_);
    }

private:
    std::vector<TStructField> Fields_;
    THashMap<TStringBuf, int> NameToFieldIndex_;
};

class TLogicalTypeWrapper
{
public:
    TLogicalTypeWrapper() = default;

    explicit TLogicalTypeWrapper(const TLogicalType& type)
        : Type_(&type)
    { }

    explicit TLogicalTypeWrapper(const TLogicalTypePtr& type)
        : Type_(type.Get())
    { }

    const TLogicalType& Type() const
    {
        YT_VERIFY(Type_);
        return *Type_;
    }

    const TStructDescriptor& StructDescriptor() const
    {
        if (!StructDescriptor_) {
            StructDescriptor_ = TStructDescriptor(Unwrap(Type()).AsStructTypeRef());
        }
        return *StructDescriptor_;
    }

private:
    const TLogicalType* Type_ = nullptr;
    mutable std::optional<TStructDescriptor> StructDescriptor_;
};

class TFieldSerializer final
    : private TStringStream
    , public TYsonWriter
{
public:
    explicit TFieldSerializer(const TLogicalType& type)
        : TYsonWriter(static_cast<TStringStream*>(this))
    {
        TypeStack_.emplace(type);
    }

    void OnStringScalar(TStringBuf value) override
    {
        ValidateSimpleType(CurrentType(), value);

        TYsonWriter::OnStringScalar(value);
    }

    void OnInt64Scalar(i64 value) override
    {
        ValidateSimpleType(CurrentType(), value);

        TYsonWriter::OnInt64Scalar(value);
    }

    void OnUint64Scalar(ui64 value) override
    {
        ValidateSimpleType(CurrentType(), value);

        TYsonWriter::OnUint64Scalar(value);
    }

    void OnDoubleScalar(double value) override
    {
        ValidateSimpleType(CurrentType(), value);

        TYsonWriter::OnDoubleScalar(value);
    }

    void OnBooleanScalar(bool value) override
    {
        ValidateSimpleType(CurrentType(), value);

        TYsonWriter::OnBooleanScalar(value);
    }

    void OnEntity() override
    {
        THROW_ERROR_EXCEPTION_UNLESS(CurrentType().IsNullable(),
            "Expected optional, but got %v", CurrentType().GetMetatype());

        TYsonWriter::OnEntity();
    }

    void OnBeginList() override
    {
        TypeStack_.emplace(UnwrapCurrentType().AsListTypeRef().GetElement());

        TYsonWriter::OnBeginList();
    }

    void OnListItem() override
    {
        TYsonWriter::OnListItem();
    }

    void OnEndList() override
    {
        TYsonWriter::OnEndList();

        TypeStack_.pop();
    }

    void OnBeginMap() override
    {
        TYsonWriter::OnBeginMap();

        TypeStack_.emplace();
    }

    void OnKeyedItem(TStringBuf key) override
    {
        TypeStack_.pop();

        if (UnwrapCurrentType().GetMetatype() == ELogicalMetatype::Dict) {
            if (key == "key") {
                TypeStack_.emplace(UnwrapCurrentType().AsDictTypeRef().GetKey());
            } else {
                TypeStack_.emplace(UnwrapCurrentType().AsDictTypeRef().GetValue());
            }

            TYsonWriter::OnKeyedItem(key);
        } else {
            const auto& structDesc = CurrentTypeWrapper().StructDescriptor();

            const int fieldId = structDesc.GetFieldIdOrThrow(key);

            TypeStack_.emplace(structDesc.GetFieldTypeOrThrow(fieldId));

            std::array<char, MaxVarInt32Size> buffer;
            const auto bufferSize = WriteVarInt32(buffer.data(), fieldId);

            TYsonWriter::OnKeyedItem(TStringBuf(buffer.data(), bufferSize));
        }
    }

    void OnEndMap() override
    {
        TypeStack_.pop();

        TYsonWriter::OnEndMap();
    }

    void OnBeginAttributes() override
    {
        THROW_ERROR_EXCEPTION("YSON attributes are not supported");
    }

    void OnEndAttributes() override
    {
        YT_ABORT();
    }

    void OnRaw(TStringBuf, EYsonType) override
    {
        THROW_ERROR_EXCEPTION("OnRaw is not supported");
    }

    TString Build() &&
    {
        TYsonWriter::Flush();
        return TStringStream::Str();
    }

private:
    struct TFieldDescriptor
    {
        ui64 Index;
        TLogicalTypePtr Type;
    };

private:
    const TLogicalTypeWrapper& CurrentTypeWrapper() const
    {
        return TypeStack_.top();
    }

    const TLogicalType& CurrentType() const
    {
        return CurrentTypeWrapper().Type();
    }

    const TLogicalType& UnwrapCurrentType() const
    {
        return Unwrap(CurrentType());
    }

private:
    std::stack<TLogicalTypeWrapper> TypeStack_;
};

class TStructSerializer final
    : public TYsonConsumerBase
{
public:
    explicit TStructSerializer(TLogicalTypePtr logicalType)
        : LogicalType_(std::move(logicalType))
        , StructDescriptor_(LogicalType_->AsStructTypeRef())
        , Buffer_(New<TRowBuffer>())
    {
        Values_.reserve(StructDescriptor_.GetCount());
        for (int i = 0; i < StructDescriptor_.GetCount(); ++i) {
            Values_.push_back(MakeUnversionedNullValue(i));
        }
    }

    void OnStringScalar(TStringBuf value) override
    {
        if (StructLevel()) {
            ValidateSimpleType(CurrentFieldType(), value);

            AddValue(MakeUnversionedStringValue(value, FieldId_));
        } else {
            FieldSerializer_->OnStringScalar(value);
        }
    }

    void OnInt64Scalar(i64 value) override
    {
        if (StructLevel()) {
            ValidateSimpleType(CurrentFieldType(), value);

            AddValue(MakeUnversionedInt64Value(value, FieldId_));
        } else {
            FieldSerializer_->OnInt64Scalar(value);
        }
    }

    void OnUint64Scalar(ui64 value) override
    {
        if (StructLevel()) {
            ValidateSimpleType(CurrentFieldType(), value);

            AddValue(MakeUnversionedUint64Value(value, FieldId_));
        } else {
            FieldSerializer_->OnUint64Scalar(value);
        }
    }

    void OnDoubleScalar(double value) override
    {
        if (StructLevel()) {
            ValidateSimpleType(CurrentFieldType(), value);

            AddValue(MakeUnversionedDoubleValue(value, FieldId_));
        } else {
            FieldSerializer_->OnDoubleScalar(value);
        }
    }

    void OnBooleanScalar(bool value) override
    {
        if (StructLevel()) {
            ValidateSimpleType(CurrentFieldType(), value);

            AddValue(MakeUnversionedBooleanValue(value, FieldId_));
        } else {
            FieldSerializer_->OnBooleanScalar(value);
        }
    }

    void OnEntity() override
    {
        if (StructLevel()) {
            THROW_ERROR_EXCEPTION_UNLESS(CurrentFieldType().IsNullable(),
                "Expected optional, but got %v", CurrentFieldType().GetMetatype());

            AddValue(MakeUnversionedSentinelValue(EValueType::Null, FieldId_));
        } else {
            FieldSerializer_->OnEntity();
        }
    }

    void OnBeginList() override
    {
        ++Depth_;

        THROW_ERROR_EXCEPTION_IF(StructLevel(), "Structure should be a map, not a list");

        if (FieldLevel()) {
            FieldSerializer_ = std::make_unique<TFieldSerializer>(CurrentFieldType());
        }

        FieldSerializer_->OnBeginList();
    }

    void OnListItem() override
    {
        FieldSerializer_->OnListItem();
    }

    void OnEndList() override
    {
        if (!StructLevel()) {
            FieldSerializer_->OnEndList();
        }

        if (FieldLevel()) {
            auto value = std::move(*FieldSerializer_).Build();
            FieldSerializer_.reset();
            AddValue(MakeUnversionedAnyValue(value, FieldId_));
        }

        --Depth_;
    }

    void OnBeginMap() override
    {
        ++Depth_;

        if (StructLevel()) {
            return;
        }

        if (FieldLevel()) {
            FieldSerializer_ = std::make_unique<TFieldSerializer>(CurrentFieldType());
        }

        FieldSerializer_->OnBeginMap();
    }

    void OnKeyedItem(TStringBuf key) override
    {
        if (StructLevel()) {
            FieldId_ = StructDescriptor_.GetFieldIdOrThrow(key);
        } else {
            FieldSerializer_->OnKeyedItem(key);
        }
    }

    void OnEndMap() override
    {
        if (!StructLevel()) {
            FieldSerializer_->OnEndMap();
        }

        if (FieldLevel()) {
            auto value = std::move(*FieldSerializer_).Build();
            FieldSerializer_.reset();
            AddValue(MakeUnversionedAnyValue(value, FieldId_));
        }

        --Depth_;
    }

    void OnBeginAttributes() override
    {
        THROW_ERROR_EXCEPTION("YSON attributes are not supported");
    }

    void OnEndAttributes() override
    {
        YT_ABORT();
    }

    using TYsonConsumerBase::OnRaw;
    void OnRaw(TStringBuf yson, EYsonType type) override
    {
        if (StructLevel()) {
            THROW_ERROR_EXCEPTION("OnRaw is not supported");
        } else {
            FieldSerializer_->OnRaw(yson, type);
        }
    }

    TUnversionedOwningRow Build()
    {
        return TRange(std::move(Values_));
    }

private:
    bool StructLevel() const
    {
        return Depth_ == 1;
    }

    bool FieldLevel() const
    {
        return Depth_ == 2;
    }

    const TLogicalType& CurrentFieldType() const
    {
        return *StructDescriptor_.GetFieldTypeOrThrow(FieldId_);
    }

private:
    const TLogicalTypePtr LogicalType_;
    const TStructDescriptor StructDescriptor_;

    std::unique_ptr<TFieldSerializer> FieldSerializer_;

    const NTableClient::TRowBufferPtr Buffer_;
    std::vector<NTableClient::TUnversionedValue> Values_;

    int Depth_ = 0;
    int FieldId_ = 0;

private:
    void AddValue(TUnversionedValue&& value)
    {
        const auto id = value.Id;
        Values_[id] = Buffer_->CaptureValue(std::move(value));
    }
};

class TStructDeserializer final
    : public TYsonConsumerBase
{
public:
    TStructDeserializer(const TLogicalTypePtr& logicalType, INodeFactory* factory)
        : Factory_(factory)
    {
        YT_ASSERT(logicalType);
        YT_ASSERT(Factory_);

        TypeStack_.emplace(logicalType);
    }

    void OnStringScalar(TStringBuf value) override
    {
        ValidateSimpleType(CurrentType(), value);

        auto node = Factory_->CreateString();
        node->SetValue(TString(value));
        AddNode(std::move(node), false);
    }

    void OnInt64Scalar(i64 value) override
    {
        ValidateSimpleType(CurrentType(), value);

        auto node = Factory_->CreateInt64();
        node->SetValue(value);
        AddNode(std::move(node), false);
    }

    void OnUint64Scalar(ui64 value) override
    {
        ValidateSimpleType(CurrentType(), value);

        auto node = Factory_->CreateUint64();
        node->SetValue(value);
        AddNode(std::move(node), false);
    }

    void OnDoubleScalar(double value) override
    {
        ValidateSimpleType(CurrentType(), value);

        auto node = Factory_->CreateDouble();
        node->SetValue(value);
        AddNode(std::move(node), false);
    }

    void OnBooleanScalar(bool value) override
    {
        ValidateSimpleType(CurrentType(), value);

        auto node = Factory_->CreateBoolean();
        node->SetValue(value);
        AddNode(std::move(node), false);
    }

    void OnEntity() override
    {
        THROW_ERROR_EXCEPTION_UNLESS(CurrentType().IsNullable(),
            "Expected optional, but got %v", CurrentType().GetMetatype());

        AddNode(Factory_->CreateEntity(), false);
    }

    void OnBeginList() override
    {
        TypeStack_.emplace(UnwrapCurrentType().AsListTypeRef().GetElement());

        AddNode(Factory_->CreateList(), true);
    }

    void OnListItem() override
    {
        YT_ASSERT(!Key_);
    }

    void OnEndList() override
    {
        NodeStack_.pop();

        TypeStack_.pop();
    }

    void OnBeginMap() override
    {
        TypeStack_.emplace();

        AddNode(Factory_->CreateMap(), true);
    }

    void OnKeyedItem(TStringBuf key) override
    {
        TypeStack_.pop();

        if (UnwrapCurrentType().GetMetatype() == ELogicalMetatype::Dict) {
            if (key == "key") {
                TypeStack_.emplace(UnwrapCurrentType().AsDictTypeRef().GetKey());
            } else {
                TypeStack_.emplace(UnwrapCurrentType().AsDictTypeRef().GetValue());
            }

            Key_ = TString(key);
        } else {
            int fieldId;
            ReadVarInt32(key.begin(), key.end(), &fieldId);

            TypeStack_.emplace();
            OnField(fieldId);
        }
    }

    void OnEndMap() override
    {
        NodeStack_.pop();

        TypeStack_.pop();
    }

    void OnBeginAttributes() override
    {
        THROW_ERROR_EXCEPTION("YSON attributes are not supported");
    }

    void OnEndAttributes() override
    {
        YT_ABORT();
    }

    INodePtr BuildNode(const TUnversionedRow& row)
    {
        OnBeginMap();

        for (const auto value : row) {
            switch (value.Type) {
                #define XX(type, cppType) \
                case EValueType::type: \
                    OnField(value.Id); \
                    On ## type ## Scalar(FromUnversionedValue<cppType>(value)); \
                    break;
                ITERATE_SCALAR_YTREE_NODE_TYPES(XX)
                #undef XX
                case EValueType::Any: {
                    OnField(value.Id);
                    TMemoryInput input(value.AsStringBuf());
                    TYsonPullParser parser(&input, EYsonType::Node);
                    TYsonPullParserCursor cursor(&parser);
                    cursor.TransferComplexValue(this);
                    break;
                }
                case EValueType::Null: {
                    OnField(value.Id);
                    OnEntity();
                    break;
                }
                default:
                    THROW_ERROR_EXCEPTION("Unsupported value type %Qlv", value.Type);
            }
        }

        OnEndMap();

        return ResultNode_;
    }

private:
    const TLogicalTypeWrapper& CurrentTypeWrapper() const
    {
        return TypeStack_.top();
    }

    const TLogicalType& CurrentType() const
    {
        return CurrentTypeWrapper().Type();
    }

    const TLogicalType& UnwrapCurrentType() const
    {
        return Unwrap(CurrentType());
    }

    void OnField(int fieldId)
    {
        TypeStack_.pop();

        const auto& structDesc = CurrentTypeWrapper().StructDescriptor();
        const auto& field = structDesc.GetFieldOrThrow(fieldId);

        Key_ = field.Name;

        TypeStack_.emplace(field.Type);
    }

    void AddNode(INodePtr node, bool push)
    {
        if (NodeStack_.empty()) {
            ResultNode_ = node;
        } else {
            auto collectionNode = NodeStack_.top();
            if (Key_) {
                if (!collectionNode->AsMap()->AddChild(*Key_, node)) {
                    THROW_ERROR_EXCEPTION("Duplicate key %Qv", *Key_);
                }
                Key_.reset();
            } else {
                collectionNode->AsList()->AddChild(node);
            }
        }

        if (push) {
            NodeStack_.push(node);
        }
    }

private:
    INodeFactory* const Factory_;

    std::stack<TLogicalTypeWrapper> TypeStack_;
    std::stack<INodePtr> NodeStack_;
    std::optional<TString> Key_;

    INodePtr ResultNode_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr GetYsonLogicalType(const TYsonStructPtr& ysonStruct)
{
    TStringStream ysonSchema;
    TYsonWriter consumer(&ysonSchema);
    ysonStruct->WriteSchema(&consumer);
    consumer.Flush();

    auto typeV3 = ConvertTo<TTypeV3LogicalTypeWrapper>(TYsonStringBuf(ysonSchema.Str()));
    return typeV3.LogicalType;
}

TTableSchemaPtr GetYsonSchema(const NYTree::TYsonStructPtr& ysonStruct)
{
    return ToTableSchema(GetYsonLogicalType(ysonStruct));
}

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr ToTableSchema(const TLogicalTypePtr& logicalType)
{
    if (!logicalType) {
        return nullptr;
    }

    const auto& structType = Unwrap(*logicalType).AsStructTypeRef();

    std::vector<TColumnSchema> columns;
    columns.reserve(structType.GetFields().size());
    for (const auto& field : structType.GetFields()) {
        columns.emplace_back(field.Name, field.Type);
    }

    return New<TTableSchema>(std::move(columns));
}

TLogicalTypePtr ToLogicalType(const TTableSchemaPtr& tableSchema)
{
    if (!tableSchema) {
        return nullptr;
    }

    const auto& columns = tableSchema->Columns();

    std::vector<TStructField> fields;
    fields.reserve(columns.size());
    for (const auto& column : columns) {
        fields.push_back({.Name = column.Name(), .Type = column.LogicalType()});
    }

    return New<TStructLogicalType>(std::move(fields));
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow Serialize(
    const TYsonStructPtr& ysonStruct,
    const TLogicalTypePtr& logicalType)
{
    TStructSerializer serializer(logicalType);
    ysonStruct->Save(&serializer);
    return serializer.Build();
}

TUnversionedOwningRow Serialize(
    const TYsonStructPtr& ysonStruct,
    const TTableSchemaPtr& schema)
{
    return Serialize(ysonStruct, ToLogicalType(schema));
}

void Deserialize(
    const TYsonStructPtr& ysonStruct,
    const TUnversionedRow& row,
    const TLogicalTypePtr& logicalType)
{
    TStructDeserializer deserializer(logicalType, GetEphemeralNodeFactory());

    ysonStruct->Load(deserializer.BuildNode(row));
}

void Deserialize(
    const NYTree::TYsonStructPtr& ysonStruct,
    const NTableClient::TUnversionedRow& row,
    const NTableClient::TTableSchemaPtr& schema)
{
    Deserialize(ysonStruct, row, ToLogicalType(schema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer
