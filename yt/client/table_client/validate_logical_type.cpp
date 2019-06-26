#include "validate_logical_type.h"
#include "logical_type.h"

#include <yt/core/yson/pull_parser.h>

#include <util/stream/mem.h>
#include <util/generic/adaptor.h>

namespace NYT::NTableClient {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <EValueType physicalType>
Y_FORCE_INLINE constexpr EYsonItemType ExpectedYsonItemType()
{
    if constexpr (physicalType == EValueType::Boolean) {
        return EYsonItemType::BooleanValue;
    } else if constexpr (physicalType == EValueType::Int64) {
        return EYsonItemType::Int64Value;
    } else if constexpr (physicalType == EValueType::Uint64) {
        return EYsonItemType::Uint64Value;
    } else if constexpr (physicalType == EValueType::Double) {
        return EYsonItemType::DoubleValue;
    } else if constexpr (physicalType == EValueType::String) {
        return EYsonItemType::StringValue;
    } else if constexpr (physicalType == EValueType::Null) {
        return EYsonItemType::EntityValue;
    } else {
        static_assert(physicalType == EValueType::Boolean, "Unexpected value type");
    }
}

////////////////////////////////////////////////////////////////////////////////

class TComplexLogicalTypeValidatorImpl
{
    class TFieldId;

public:
    TComplexLogicalTypeValidatorImpl(TYsonPullParser* parser, TComplexTypeFieldDescriptor descriptor)
        : Cursor_(parser)
        , RootDescriptor_(descriptor)
    { }

    void Validate()
    {
        return ValidateLogicalType(RootDescriptor_.GetType(), TFieldId());
    }

private:
    void ValidateLogicalType(const TLogicalTypePtr& type, const TFieldId& fieldId)
    {
        switch (type->GetMetatype()) {
            case ELogicalMetatype::Simple:
                ValidateSimpleType(type->AsSimpleTypeRef().GetElement(), true, fieldId);
                return;
            case ELogicalMetatype::Optional:
                ValidateOptionalType(type->AsOptionalTypeRef(), fieldId.OptionalElement());
                return;
            case ELogicalMetatype::List:
                ValidateListType(type->AsListTypeRef(), fieldId);
                return;
            case ELogicalMetatype::Struct:
                ValidateStructType(type->AsStructTypeRef(), fieldId);
                return;
            case ELogicalMetatype::Tuple:
                ValidateTupleType(type->AsTupleTypeRef(), fieldId);
                return;
        }
        YT_ABORT();
    }

    template <ESimpleLogicalValueType type, bool required>
    void ValidateSimpleType(const TFieldId& fieldId)
    {
        if constexpr (type == ESimpleLogicalValueType::Any) {
            switch (Cursor_.GetCurrent().GetType()) {
                case EYsonItemType::EntityValue:
                    if constexpr (required) {
                        THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                            "Cannot parse %Qv; unexpected entity value",
                            GetDescription(fieldId));
                    }
                    Cursor_.Next();
                    return;
                case EYsonItemType::Int64Value:
                case EYsonItemType::BooleanValue:
                case EYsonItemType::Uint64Value:
                case EYsonItemType::DoubleValue:
                case EYsonItemType::StringValue:
                    Cursor_.Next();
                    return;
                case EYsonItemType::BeginAttributes:
                    THROW_ERROR_EXCEPTION(
                        EErrorCode::SchemaViolation,
                        "Cannot parse %Qv; unexpected top level attributes",
                        GetDescription(fieldId));

                case EYsonItemType::BeginList:
                case EYsonItemType::BeginMap: {
                    Cursor_.SkipComplexValue();
                    return;
                }
                default:
                    YT_ABORT();
            }
        } else {
            static_assert(type != ESimpleLogicalValueType::Any);
            constexpr auto expectedYsonEventType = ExpectedYsonItemType<GetPhysicalType(type)>();
            if (Cursor_.GetCurrent().GetType() != expectedYsonEventType) {
                if (required) {
                    THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                        "Cannot parse %Qv; expected: %Qv found: %Qv",
                        GetDescription(fieldId),
                        expectedYsonEventType,
                        Cursor_.GetCurrent().GetType());
                } else if (Cursor_.GetCurrent().GetType() == EYsonItemType::EntityValue) {
                    YT_ASSERT(!required);
                    Cursor_.Next();
                    return;
                } else {
                    THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                        "Cannot parse %Qv; expected: %Qv or %Qv found: %Qv",
                        GetDescription(fieldId),
                        expectedYsonEventType,
                        EYsonItemType::EntityValue,
                        Cursor_.GetCurrent().GetType());
                }
            }

            if constexpr (expectedYsonEventType == EYsonItemType::EntityValue) {
                // nothing to check
            } else if constexpr (expectedYsonEventType == EYsonItemType::BooleanValue) {
                NTableClient::ValidateSimpleLogicalType<type>(Cursor_.GetCurrent().UncheckedAsBoolean());
            } else if constexpr (expectedYsonEventType == EYsonItemType::Int64Value) {
                NTableClient::ValidateSimpleLogicalType<type>(Cursor_.GetCurrent().UncheckedAsInt64());
            } else if constexpr (expectedYsonEventType == EYsonItemType::Uint64Value) {
                NTableClient::ValidateSimpleLogicalType<type>(Cursor_.GetCurrent().UncheckedAsUint64());
            } else if constexpr (expectedYsonEventType == EYsonItemType::DoubleValue) {
                NTableClient::ValidateSimpleLogicalType<type>(Cursor_.GetCurrent().UncheckedAsDouble());
            } else if constexpr (expectedYsonEventType == EYsonItemType::StringValue) {
                NTableClient::ValidateSimpleLogicalType<type>(Cursor_.GetCurrent().UncheckedAsString());
            } else {
                static_assert(expectedYsonEventType == EYsonItemType::EntityValue, "unexpected EYsonItemType");
            }
            Cursor_.Next();
        }
    }

    Y_FORCE_INLINE void ValidateSimpleType(ESimpleLogicalValueType type, bool required, const TFieldId& fieldId)
    {
        switch (type) {
#define CASE(x) \
            case x: \
                    if (required) { \
                        ValidateSimpleType<x, true>(fieldId); \
                    } else { \
                        ValidateSimpleType<x, false>(fieldId); \
                    } \
                    return;
            CASE(ESimpleLogicalValueType::Null)
            CASE(ESimpleLogicalValueType::Int64)
            CASE(ESimpleLogicalValueType::Uint64)
            CASE(ESimpleLogicalValueType::Double)
            CASE(ESimpleLogicalValueType::Boolean)
            CASE(ESimpleLogicalValueType::String)
            CASE(ESimpleLogicalValueType::Any)
            CASE(ESimpleLogicalValueType::Int8)
            CASE(ESimpleLogicalValueType::Uint8)
            CASE(ESimpleLogicalValueType::Int16)
            CASE(ESimpleLogicalValueType::Uint16)
            CASE(ESimpleLogicalValueType::Int32)
            CASE(ESimpleLogicalValueType::Uint32)
            CASE(ESimpleLogicalValueType::Utf8)
#undef CASE
        }
        YT_ABORT();
    }

    void ValidateOptionalType(const TOptionalLogicalType& type, const TFieldId& fieldId)
    {
        if (Cursor_.GetCurrent().GetType() == EYsonItemType::EntityValue) {
            Cursor_.Next();
            return;
        }

        if (type.GetElement()->GetMetatype() != ELogicalMetatype::Optional) {
            ValidateLogicalType(type.GetElement(), fieldId.OptionalElement());
            return;
        }

        if (Cursor_.GetCurrent().GetType() != EYsonItemType::BeginList) {
            THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                "Cannot parse %Qv; expected: %Qv found: %Qv",
                GetDescription(fieldId),
                EYsonItemType::BeginList,
                Cursor_.GetCurrent().GetType());
        }
        Cursor_.Next();
        if (Cursor_.GetCurrent().GetType() == EYsonItemType::EndList) {
            THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                "Cannot parse %Qv; empty yson",
                GetDescription(fieldId),
                Cursor_.GetCurrent().GetType());
        }
        ValidateLogicalType(type.GetElement(), fieldId.OptionalElement());
        if (Cursor_.GetCurrent().GetType() != EYsonItemType::EndList) {
            THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                "Cannot parse %Qv; expected: %Qv found: %Qv",
                GetDescription(fieldId),
                EYsonItemType::EndList,
                Cursor_.GetCurrent().GetType());
        }
        Cursor_.Next();
    }

    void ValidateListType(const TListLogicalType& type, const TFieldId& fieldId)
    {
        if (Cursor_.GetCurrent().GetType() != EYsonItemType::BeginList) {
            THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                "Cannot parse %Qv; expected: %Qv found: %Qv",
                GetDescription(fieldId),
                EYsonItemType::BeginList,
                Cursor_.GetCurrent().GetType());
        }
        Cursor_.Next();
        const auto& elementType = type.GetElement();
        auto elementFieldId = fieldId.ListElement();
        while (Cursor_.GetCurrent().GetType() != EYsonItemType::EndList) {
            ValidateLogicalType(elementType, elementFieldId);
        }
        Cursor_.Next();
    }

    void ValidateStructType(const TStructLogicalType& type, const TFieldId& fieldId)
    {
        if (Cursor_.GetCurrent().GetType() != EYsonItemType::BeginList) {
            THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                "Cannot parse %Qv; expected: %Qv found: %Qv",
                GetDescription(fieldId),
                EYsonItemType::BeginList,
                Cursor_.GetCurrent().GetType());
        }
        Cursor_.Next();
        const auto& fields = type.GetFields();
        for (size_t i = 0; i < fields.size(); ++i) {
            if (Cursor_.GetCurrent().GetType() == EYsonItemType::EndList) {
                do {
                    const auto& field = fields[i];
                    if (field.Type->GetMetatype() != ELogicalMetatype::Optional) {
                        THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                            "Cannot parse %Qv; struct ended before required field %Qv is set",
                            GetDescription(fieldId),
                            field.Name);
                    }
                    ++i;
                } while (i < fields.size());
                break;
            }
            const auto& field = fields[i];
            ValidateLogicalType(field.Type, fieldId.StructField(i));
        }
        if (Cursor_.GetCurrent().GetType() != EYsonItemType::EndList) {
            THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                "Cannot parse %Qv; expected: %Qv found: %Qv",
                GetDescription(fieldId),
                EYsonItemType::EndList,
                Cursor_.GetCurrent().GetType());
        }
        Cursor_.Next();
    }

    void ValidateTupleType(const TTupleLogicalType& type, const TFieldId& fieldId)
    {
        if (Cursor_.GetCurrent().GetType() != EYsonItemType::BeginList) {
            THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                "Cannot parse %Qv; expected: %Qv found: %Qv",
                GetDescription(fieldId),
                EYsonItemType::BeginList,
                Cursor_.GetCurrent().GetType());
        }
        Cursor_.Next();
        const auto& elements = type.GetElements();
        for (size_t i = 0; i < elements.size(); ++i) {
            if (Cursor_.GetCurrent().GetType() == EYsonItemType::EndList) {
                THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                    "Cannot parse %Qv; expected %Qv got %Qv",
                    GetDescription(fieldId),
                    GetDescription(fieldId.TupleElement(i)),
                    EYsonItemType::EndList);
            }
            ValidateLogicalType(elements[i], fieldId.TupleElement(i));
        }
        if (Cursor_.GetCurrent().GetType() != EYsonItemType::EndList) {
            THROW_ERROR_EXCEPTION(EErrorCode::SchemaViolation,
                "Cannot parse %Qv; expected: %Qv found: %Qv",
                GetDescription(fieldId),
                EYsonItemType::EndList,
                Cursor_.GetCurrent().GetType());
        }
        Cursor_.Next();
    }

    TString GetDescription(const TFieldId& fieldId) const
    {
        return fieldId.GetDescriptor(RootDescriptor_).GetDescription();
    }

private:
    class TFieldId
    {
    public:
        // Root field id
        TFieldId() = default;

        TFieldId OptionalElement() const
        {
            return {this, 0};
        }

        TFieldId ListElement() const
        {
            return {this, 0};
        }

        TFieldId StructField(int i) const
        {
            return {this, i};
        }

        TFieldId TupleElement(int i) const
        {
            return {this, i};
        }

        TComplexTypeFieldDescriptor GetDescriptor(const TComplexTypeFieldDescriptor& root) const
        {
            std::vector<int> path;
            const auto* current = this;
            while (current->Parent_ != nullptr) {
                path.push_back(current->SiblingIndex_);
                current = current->Parent_;
            }

            auto descriptor = root;
            for (const auto& childIndex : Reversed(path)) {
                const auto& type = descriptor.GetType();
                switch (type->GetMetatype()) {
                    case ELogicalMetatype::Simple:
                        return descriptor;
                    case ELogicalMetatype::Optional:
                        descriptor = descriptor.OptionalElement();
                        continue;
                    case ELogicalMetatype::List:
                        descriptor = descriptor.ListElement();
                        continue;
                    case ELogicalMetatype::Struct:
                        descriptor = descriptor.StructField(childIndex);
                        continue;
                    case ELogicalMetatype::Tuple:
                        descriptor = descriptor.TupleElement(childIndex);
                        continue;
                }
                YT_ABORT();
            }
            return descriptor;
        }

    private:
        TFieldId(const TFieldId* parent, int siblingIndex)
            : Parent_(parent)
            , SiblingIndex_(siblingIndex)
        { }

    private:
        const TFieldId* Parent_ = nullptr;
        int SiblingIndex_ = 0;
    };

private:
    TYsonPullParserCursor Cursor_;
    const TComplexTypeFieldDescriptor RootDescriptor_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ValidateComplexLogicalType(TStringBuf ysonData, const TLogicalTypePtr& type)
{
    TMemoryInput in(ysonData);
    TYsonPullParser parser(&in, EYsonType::Node);
    TComplexLogicalTypeValidatorImpl validator(&parser, TComplexTypeFieldDescriptor(type));
    validator.Validate();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
