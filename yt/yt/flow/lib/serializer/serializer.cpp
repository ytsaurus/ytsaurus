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

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFieldSerializer final
    : private TStringStream
    , public TYsonWriter
{
public:
     TFieldSerializer()
        : TYsonWriter(static_cast<TStringStream*>(this))
    { }

    TString Build() &&
    {
        TYsonWriter::Flush();
        return TStringStream::Str();
    }
};

class TStructSerializer final
    : public TYsonConsumerBase
{
public:
    explicit TStructSerializer(TTableSchemaPtr schema)
        : Schema_(std::move(schema))
        , Buffer_(New<TRowBuffer>())
    {
        Values_.reserve(Schema_->GetColumnCount());
        for (int i = 0; i < Schema_->GetColumnCount(); ++i) {
            Values_.push_back(MakeUnversionedNullValue(i));
        }
    }

    void OnStringScalar(TStringBuf value) override
    {
        if (StructLevel() && FieldId_) {
            AddValue(MakeUnversionedStringValue(value, *FieldId_));
        } else if (!StructLevel() && FieldSerializer_) {
            FieldSerializer_->OnStringScalar(value);
        }
    }

    void OnInt64Scalar(i64 value) override
    {
        if (StructLevel() && FieldId_) {
            AddValue(MakeUnversionedInt64Value(value, *FieldId_));
        } else if (!StructLevel() && FieldSerializer_) {
            FieldSerializer_->OnInt64Scalar(value);
        }
    }

    void OnUint64Scalar(ui64 value) override
    {
        if (StructLevel() && FieldId_) {
            AddValue(MakeUnversionedUint64Value(value, *FieldId_));
        } else if (!StructLevel() && FieldSerializer_) {
            FieldSerializer_->OnUint64Scalar(value);
        }
    }

    void OnDoubleScalar(double value) override
    {
        if (StructLevel() && FieldId_) {
            AddValue(MakeUnversionedDoubleValue(value, *FieldId_));
        } else if (!StructLevel() && FieldSerializer_) {
            FieldSerializer_->OnDoubleScalar(value);
        }
    }

    void OnBooleanScalar(bool value) override
    {
        if (StructLevel() && FieldId_) {
            AddValue(MakeUnversionedBooleanValue(value, *FieldId_));
        } else if (!StructLevel() && FieldSerializer_) {
            FieldSerializer_->OnBooleanScalar(value);
        }
    }

    void OnEntity() override
    {
        if (StructLevel() && FieldId_) {
            AddValue(MakeUnversionedSentinelValue(EValueType::Null, *FieldId_));
        } else if (!StructLevel() && FieldSerializer_) {
            FieldSerializer_->OnEntity();
        }
    }

    void OnBeginList() override
    {
        ++Depth_;

        THROW_ERROR_EXCEPTION_IF(StructLevel(), "Structure should be a map, not a list");

        if (FieldLevel() && FieldId_) {
            FieldSerializer_ = std::make_unique<TFieldSerializer>();
        }

        if (FieldSerializer_) {
            FieldSerializer_->OnBeginList();
        }
    }

    void OnListItem() override
    {
        YT_VERIFY(!StructLevel());

        if (FieldSerializer_) {
            FieldSerializer_->OnListItem();
        }
    }

    void OnEndList() override
    {
        YT_VERIFY(!StructLevel());

        if (FieldSerializer_) {
            if (!StructLevel()) {
                FieldSerializer_->OnEndList();
            }

            if (FieldLevel()) {
                auto value = std::move(*FieldSerializer_).Build();
                FieldSerializer_.reset();
                AddValue(MakeUnversionedAnyValue(value, *FieldId_));
            }
        }

        --Depth_;
    }

    void OnBeginMap() override
    {
        ++Depth_;

        if (StructLevel()) {
            return;
        }

        if (FieldLevel() && FieldId_) {
            FieldSerializer_ = std::make_unique<TFieldSerializer>();
        }

        if (FieldSerializer_) {
            FieldSerializer_->OnBeginMap();
        }
    }

    void OnKeyedItem(TStringBuf key) override
    {
        if (StructLevel()) {
            if (const auto* column = Schema_->FindColumn(key)) {
                FieldId_ = Schema_->GetColumnIndex(*column);
            } else {
                FieldId_ = {};
            }
        } else if (FieldSerializer_) {
            FieldSerializer_->OnKeyedItem(key);
        }
    }

    void OnEndMap() override
    {
        if (!StructLevel() && FieldSerializer_) {
            FieldSerializer_->OnEndMap();
        }

        if (FieldLevel() && FieldId_) {
            auto value = std::move(*FieldSerializer_).Build();
            FieldSerializer_.reset();
            AddValue(MakeUnversionedAnyValue(value, *FieldId_));
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

private:
    const TTableSchemaPtr Schema_;
    std::unique_ptr<TFieldSerializer> FieldSerializer_;

    const TRowBufferPtr Buffer_;
    std::vector<TUnversionedValue> Values_;

    int Depth_ = 0;
    std::optional<int> FieldId_;

private:
    void AddValue(TUnversionedValue&& value)
    {
        const auto id = value.Id;
        const auto& column = Schema_->Columns()[id];
        ValidateValueType(value, column, false, false, false);
        Values_[id] = Buffer_->CaptureValue(std::move(value));
    }
};

////////////////////////////////////////////////////////////////////////////////

INodePtr ConvertUnversionedValueToNode(const TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Null:
            return nullptr;
        case EValueType::Int64:
            return ConvertToNode(FromUnversionedValue<i64>(value));
        case EValueType::Uint64:
            return ConvertToNode(FromUnversionedValue<ui64>(value));
        case EValueType::Double:
            return ConvertToNode(FromUnversionedValue<double>(value));
        case EValueType::Boolean:
            return ConvertToNode(FromUnversionedValue<bool>(value));
        case EValueType::String:
            return ConvertToNode(FromUnversionedValue<TString>(value));
        case EValueType::Any:
            return ConvertToNode(FromUnversionedValue<TYsonString>(value));
        case EValueType::Min:
        case EValueType::TheBottom:
        case EValueType::Composite:
        case EValueType::Max:
            THROW_ERROR_EXCEPTION("Unsupported type %Qv", value.Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr GetYsonTableSchema(const TYsonStructPtr& ysonStruct)
{
    std::vector<TColumnSchema> columns;

    const auto* meta = ysonStruct->GetMeta();
    for (const auto& [key, parameter] : meta->GetParameterMap()) {
        TStringStream parameterSchema;
        TYsonWriter consumer(&parameterSchema);
        parameter->WriteSchema(ysonStruct.Get(), &consumer);
        consumer.Flush();
        auto logicalType = ConvertTo<TTypeV3LogicalTypeWrapper>(TYsonStringBuf(parameterSchema.Str())).LogicalType;
        auto [type, required] = CastToV1Type(logicalType);
        columns.push_back(TColumnSchema(key, type).SetRequired(required));
    }
    std::sort(columns.begin(), columns.end(), [](const auto& l, const auto& r) {
        return l.Name() < r.Name();
    });
    return New<TTableSchema>(std::move(columns));
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow Serialize(const TYsonStructPtr& ysonStruct, const TTableSchemaPtr& schema)
{
    TStructSerializer serializer(schema);
    ysonStruct->Save(&serializer);
    return serializer.Build();
}

void Deserialize(const TYsonStructPtr& ysonStruct, const TUnversionedRow& row, const TTableSchemaPtr& schema)
{
    YT_ASSERT(row.GetCount() == (ui32)schema->GetColumnCount());

    INodePtr node = GetEphemeralNodeFactory()->CreateMap();
    auto mapNode = node->AsMap();
    for (int i = 0; i < schema->GetColumnCount(); ++i) {
        const auto& column = schema->Columns()[i];
        const auto& value = row[i];
        auto child = ConvertUnversionedValueToNode(value);
        // we ignore null values
        if (child) {
            mapNode->AddChild(column.Name(), child);
        }
    }

    ysonStruct->Load(node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer
