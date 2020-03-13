#include "skiff_yson_converter.h"

#include <yt/client/table_client/logical_type.h>

#include <yt/library/skiff/skiff.h>
#include <yt/library/skiff/skiff_schema.h>
#include <yt/core/yson/pull_parser.h>
#include <yt/core/yson/parser.h>
#include <yt/core/yson/token_writer.h>

#include <util/stream/zerocopy.h>

namespace NYT::NFormats {

using namespace NSkiff;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

EWireType GetSkiffTypeForSimpleLogicalType(ESimpleLogicalValueType logicalType)
{
    const auto valueType = GetPhysicalType(logicalType);
    switch (valueType) {
        case EValueType::Int64:
            return EWireType::Int64;
        case EValueType::Uint64:
            return EWireType::Uint64;
        case EValueType::String:
            return EWireType::String32;
        case EValueType::Any:
            return EWireType::Yson32;
        case EValueType::Boolean:
            return EWireType::Boolean;
        case EValueType::Double:
            return EWireType::Double;
        case EValueType::Null:
            return EWireType::Nothing;
        case EValueType::Composite:
            // NB. GetPhysicalType never returns EValueType::Composite
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            break;
    }
    ThrowUnexpectedValueType(valueType);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using TTypePair = std::pair<TComplexTypeFieldDescriptor, TSkiffSchemaPtr>;

struct TConverterCreationContext;

TYsonToSkiffConverter CreateYsonToSkiffConverterImpl(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TYsonToSkiffConverterConfig& config);

TSkiffToYsonConverter CreateSkiffToYsonConverterImpl(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TSkiffToYsonConverterConfig& config);

////////////////////////////////////////////////////////////////////////////////

struct TConverterCreationContext
{
    int NestingLevel = 0;
};

std::vector<TErrorAttribute> SkiffYsonErrorAttributes(const TComplexTypeFieldDescriptor& descriptor, const TSkiffSchemaPtr& skiffSchema)
{
    return {
        TErrorAttribute("complex_type_field", descriptor.GetDescription()),
        TErrorAttribute("logical_type_dbg", ToString(*descriptor.GetType())),
        TErrorAttribute("skiff_schema_dbg", GetShortDebugString(skiffSchema)),
    };
}

TSkiffSchemaPtr GetOptionalChild(const TSkiffSchemaPtr& skiffSchema)
{
    if (skiffSchema->GetWireType() != EWireType::Variant8) {
        return nullptr;
    }
    auto children = skiffSchema->GetChildren();
    if (children.size() != 2) {
        return nullptr;
    }
    if (children[0]->GetWireType() != EWireType::Nothing) {
        return nullptr;
    }
    return children[1];
}

struct TSkiffStructField
{
    TString Name;
    TSkiffSchemaPtr Type;
};

template<EWireType wireType>
constexpr EYsonItemType WireTypeToYsonItemType()
{
    if constexpr (wireType == EWireType::Int64) {
        return EYsonItemType::Int64Value;
    } else if constexpr (wireType == EWireType::Uint64) {
        return EYsonItemType::Uint64Value;
    } else if constexpr (wireType == EWireType::Double) {
        return EYsonItemType::DoubleValue;
    } else if constexpr (wireType == EWireType::Boolean) {
        return EYsonItemType::BooleanValue;
    } else if constexpr (wireType == EWireType::String32) {
        return EYsonItemType::StringValue;
    } else if constexpr (wireType == EWireType::Nothing) {
        return EYsonItemType::EntityValue;
    } else {
        static_assert(wireType == EWireType::Int64);
    }
}

struct TOptionalTypesMatch
{
    TTypePair InnerTypes;
    int LogicalNesting = 0;
    int SkiffNesting = 0;
};

[[noreturn]] void ThrowBadWireType(EWireType expected, EWireType actual)
{
    THROW_ERROR_EXCEPTION("Bad skiff wire type, expected: %Qlv, actual: %Qlv",
        expected,
        actual);
}

[[noreturn]] void RethrowCannotMatchField(
    const TComplexTypeFieldDescriptor& descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const std::exception& ex)
{
    THROW_ERROR_EXCEPTION("Cannot match field %Qv to skiff schema",
        descriptor.GetDescription())
        << SkiffYsonErrorAttributes(descriptor, skiffSchema)
        << ex;
}

template <typename... Args>
[[noreturn]] void ThrowYsonToSkiffConversionError(const TComplexTypeFieldDescriptor& descriptor, const Args&... args)
{
    THROW_ERROR_EXCEPTION("Yson to Skiff conversion error while converting %Qv field",
        descriptor.GetDescription())
        << TError(args...);
}

[[noreturn]] void ThrowBadYsonToken(
    const TComplexTypeFieldDescriptor& descriptor,
    const std::vector<EYsonItemType>& expected,
    const EYsonItemType actual)
{
    TStringStream expectationString;
    if (expected.size() > 1) {
        expectationString << "one of ";
        bool first = true;
        for (const auto& itemType : expected) {
            if (!first) {
                expectationString << ", ";
            }
            first = false;
            expectationString << Format("%Qlv", itemType);
        }
    } else {
        YT_VERIFY(expected.size() == 1);
        expectationString << Format("%Qlv", expected[0]);
    }

    ThrowYsonToSkiffConversionError(descriptor, "Bad yson token type, expected %v actual: %Qlv",
        expectationString.Str(),
        actual);
}

template <typename... Args>
[[noreturn]] void ThrowSkiffToYsonConversionError(const TComplexTypeFieldDescriptor& descriptor, const Args&... args)
{
    THROW_ERROR_EXCEPTION("Skiff to Yson conversion error while converting %Qv field",
        descriptor.GetDescription())
        << TError(args...);
}

TOptionalTypesMatch MatchOptionalTypes(
    const TComplexTypeFieldDescriptor& descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    bool allowOmitOptional)
{
    // NB. Here we have a problem:
    // variant_tuple<null, T> has exactly the same skiff representation as optional<T>.
    // We need to perform nontrivial analysis of schemas in order to align these schemas.
    //
    // When reading this code you can keep in mind following examples
    //   1. logical type: optional<varaint_tuple<null, T>>
    //      skiff type: variant8<nothing, variant8<nothing, T>>
    //      Outer skiff `variant8` encodes logical outer `optional` type
    //   2. logical type: optional<variant_tuple<null, T>>
    //      skiff type: variant8<nothing, T>
    //      Outer `variant8` skiff type encodes logical inner `variant_tuple` type when allowOmitOptional is true.

    // When logical type is either optional<T> or variant_tuple<null, T> this function returns descriptor of T.
    // Otherwise it returns std::nullopt.
    const auto& getLogicalOptionalLikeChild = [] (const TComplexTypeFieldDescriptor& descriptor) -> std::optional<TComplexTypeFieldDescriptor> {
        const auto& type = descriptor.GetType();
        switch (type->GetMetatype()) {
            case ELogicalMetatype::Optional:
                return descriptor.OptionalElement();
            case ELogicalMetatype::VariantTuple: {
                const auto& variantType = type->AsVariantTupleTypeRef();
                if (variantType.GetElements().size() == 2
                    && *variantType.GetElements()[0] == *SimpleLogicalType(ESimpleLogicalValueType::Null))
                {
                    return descriptor.VariantTupleElement(1);
                } else {
                    return {};
                }
            }
            default:
                return {};
        }
    };

    try {

        // First of all we compute sctrict and relaxed depths of optional chain.
        // Strict depth is the depth of chain where each element is optinonal<T>.
        // Relaxed depth is the depth of chain where each element is optional<T> or variant<null, T>

        int logicalNestingRelaxed = 0;
        int logicalNestingStrict = 0;
        {
            auto innerDescriptor = descriptor;
            while (auto element = getLogicalOptionalLikeChild(innerDescriptor)) {
                if (innerDescriptor.GetType()->GetMetatype() == ELogicalMetatype::Optional &&
                    logicalNestingStrict == logicalNestingRelaxed) {
                    ++logicalNestingStrict;
                }
                ++logicalNestingRelaxed;
                innerDescriptor = *element;
            }
        }
        YT_VERIFY(logicalNestingStrict);
        YT_VERIFY(logicalNestingRelaxed >= logicalNestingStrict);

        // Then we compute depth of corresponding skiff chain of variant<nothing, T>
        // This chain should match to logical relaxed chain.
        int skiffNestingRelaxed = 0;
        {
            TSkiffSchemaPtr innerSkiffSchema = skiffSchema;
            while (auto child = GetOptionalChild(innerSkiffSchema)) {
                ++skiffNestingRelaxed;
                innerSkiffSchema = child;
            }
        }

        if (logicalNestingRelaxed != skiffNestingRelaxed &&
            !(allowOmitOptional && logicalNestingRelaxed == skiffNestingRelaxed + 1))
        {
            THROW_ERROR_EXCEPTION("Optional nesting mismatch, logical type nesting: %Qv, skiff nesting: %Qv",
                logicalNestingRelaxed,
                skiffNestingRelaxed);
        }

        // NB. We only allow to omit outer optional of the column so in order to match lengths of inner chains must be
        // equal. Based on this assertion we compute lengths of skiff chain corresponding to length of
        auto skiffNestingStrict = skiffNestingRelaxed - (logicalNestingRelaxed - logicalNestingStrict);
        YT_VERIFY(skiffNestingRelaxed >= 0);

        // We descend over strict chains once again to get matching inner types.
        auto innerDescriptor = descriptor;
        for (int i = 0; i < logicalNestingStrict; ++i) {
            YT_VERIFY(innerDescriptor.GetType()->GetMetatype() == ELogicalMetatype::Optional);
            innerDescriptor = innerDescriptor.OptionalElement();
        }
        YT_VERIFY(innerDescriptor.GetType()->GetMetatype() != ELogicalMetatype::Optional);

        auto innerSkiffSchema = skiffSchema;
        for (int i = 0; i < skiffNestingStrict; ++i) {
            innerSkiffSchema = GetOptionalChild(innerSkiffSchema);
            YT_VERIFY(innerSkiffSchema);
        }

        return {{std::move(innerDescriptor), innerSkiffSchema}, logicalNestingStrict, skiffNestingStrict};
    } catch (const std::exception& ex) {
        RethrowCannotMatchField(descriptor, skiffSchema, ex);
    }
}

TTypePair MatchListTypes(const TComplexTypeFieldDescriptor& descriptor, const TSkiffSchemaPtr& skiffSchema)
{
    try {
        if (skiffSchema->GetWireType() != EWireType::RepeatedVariant8) {
            ThrowBadWireType(EWireType::RepeatedVariant8, skiffSchema->GetWireType());
        }
        if (skiffSchema->GetChildren().size() != 1) {
            THROW_ERROR_EXCEPTION(
                "%Qv has too many children, expected: %Qv actual %Qv",
                skiffSchema->GetWireType(),
                1,
                skiffSchema->GetChildren().size());
        }
    } catch (const std::exception& ex) {
        RethrowCannotMatchField(descriptor, skiffSchema, ex);
    }
    return {descriptor.ListElement(), skiffSchema->GetChildren()[0]};
}

std::vector<TTypePair> MatchStructTypes(const TComplexTypeFieldDescriptor& descriptor, const TSkiffSchemaPtr& skiffSchema)
{
    try {
        if (skiffSchema->GetWireType() != EWireType::Tuple) {
            ThrowBadWireType(EWireType::Tuple, skiffSchema->GetWireType());
        }

        std::vector<TSkiffStructField> skiffFields;
        {
            const auto& children = skiffSchema->GetChildren();
            for (size_t i = 0; i < children.size(); ++i) {
                const auto& child = children[i];
                if (child->GetName().empty()) {
                    THROW_ERROR_EXCEPTION("%Qv child #%v has empty name",
                        EWireType::Tuple,
                        i);
                }
                skiffFields.push_back({child->GetName(), child});
            }
        }

        size_t skiffFieldIndex = 0;

        std::vector<TTypePair> result;
        const auto& fields = descriptor.GetType()->AsStructTypeRef().GetFields();
        for (size_t i = 0; i < fields.size(); ++i) {
            auto logicalField = fields[i];
            TSkiffSchemaPtr skiffFieldSchema = nullptr;
            if (skiffFieldIndex < skiffFields.size() && skiffFields[skiffFieldIndex].Name == logicalField.Name) {
                skiffFieldSchema = skiffFields[skiffFieldIndex].Type;
                ++skiffFieldIndex;
            }
            result.push_back({descriptor.StructField(i), skiffFieldSchema});
        }

        if (skiffFieldIndex < skiffFields.size()) {
            auto skiffName = skiffFields[skiffFieldIndex].Name;
            for (const auto& ysonField : fields) {
                if (ysonField.Name == skiffName) {
                    THROW_ERROR_EXCEPTION("%Qv child %Qv is out of order",
                        EWireType::Tuple,
                        skiffName);
                }
            }
            THROW_ERROR_EXCEPTION("%Qv child %Qv is not found in logical type",
                EWireType::Tuple,
                skiffName);
        }
        return result;
    } catch (const std::exception& ex) {
        RethrowCannotMatchField(descriptor, skiffSchema, ex);
    }
}

std::vector<TTypePair> MatchTupleTypes(const TComplexTypeFieldDescriptor& descriptor, const TSkiffSchemaPtr& skiffSchema)
{
    try {
        if (skiffSchema->GetWireType() != EWireType::Tuple) {
            ThrowBadWireType(EWireType::Tuple, skiffSchema->GetWireType());
        }

        const auto& elements = descriptor.GetType()->AsTupleTypeRef().GetElements();
        const auto& children = skiffSchema->GetChildren();

        if (children.size() != elements.size()) {
            THROW_ERROR_EXCEPTION("Tuple element counts do not match; logical type elements: %v skiff elements: %v",
                elements.size(),
                children.size());
        }

        std::vector<TTypePair> result;
        for (size_t i = 0; i < elements.size(); ++i) {
            result.push_back({descriptor.TupleElement(i), children[i]});
        }

        return result;
    } catch (const std::exception& ex) {
        RethrowCannotMatchField(descriptor, skiffSchema, ex);
    }
}

std::vector<TTypePair> MatchVariantTupleTypes(const TComplexTypeFieldDescriptor& descriptor, const TSkiffSchemaPtr& skiffSchema)
{
    try {
        if (skiffSchema->GetWireType() != EWireType::Variant8 && skiffSchema->GetWireType() != EWireType::Variant16) {
            ThrowBadWireType(EWireType::Tuple, skiffSchema->GetWireType());
        }

        const auto& elements = descriptor.GetType()->AsVariantTupleTypeRef().GetElements();
        const auto& children = skiffSchema->GetChildren();

        if (children.size() != elements.size()) {
            THROW_ERROR_EXCEPTION("Variant element counts do not match; logical type elements: %v skiff elements: %v",
                elements.size(),
                children.size());
        }

        std::vector<TTypePair> result;
        for (size_t i = 0; i < elements.size(); ++i) {
            result.push_back({descriptor.VariantTupleElement(i), children[i]});
        }

        return result;
    } catch (const std::exception& ex) {
        RethrowCannotMatchField(descriptor, skiffSchema, ex);
    }
}

std::vector<TTypePair> MatchVariantStructTypes(const TComplexTypeFieldDescriptor& descriptor, const TSkiffSchemaPtr& skiffSchema)
{
    try {
        if (skiffSchema->GetWireType() != EWireType::Variant8 && skiffSchema->GetWireType() != EWireType::Variant16) {
            ThrowBadWireType(EWireType::Variant8, skiffSchema->GetWireType());
        }

        const auto& fields = descriptor.GetType()->AsVariantStructTypeRef().GetFields();
        const auto& children = skiffSchema->GetChildren();

        if (children.size() != fields.size()) {
            THROW_ERROR_EXCEPTION("Variant element counts do not match; logical type elements: %v skiff elements: %v",
                fields.size(),
                children.size());
        }

        std::vector<TTypePair> result;
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i].Name != children[i]->GetName()) {
                THROW_ERROR_EXCEPTION("Skiff %v child #%v expected to be %Qv but %Qv found",
                    skiffSchema->GetWireType(),
                    i,
                    fields[i].Name,
                    children[i]->GetName());
            }
            result.push_back({descriptor.VariantStructField(i), children[i]});
        }

        return result;
    } catch (const std::exception& ex) {
        RethrowCannotMatchField(descriptor, skiffSchema, ex);
    }
}

std::pair<TTypePair, TTypePair> MatchDictTypes(const TComplexTypeFieldDescriptor& descriptor, const TSkiffSchemaPtr& skiffSchema)
{
    try {
        if (skiffSchema->GetWireType() != EWireType::RepeatedVariant8) {
            ThrowBadWireType(EWireType::RepeatedVariant8, skiffSchema->GetWireType());
        }

        if (skiffSchema->GetChildren().size() != 1) {
            THROW_ERROR_EXCEPTION("%Qlv has unexpected children count: %Qv expected children count: %Qv",
                EWireType::RepeatedVariant8,
                skiffSchema->GetChildren().size(),
                1);
        }

        auto tupleSchema = skiffSchema->GetChildren()[0];
        if (tupleSchema->GetWireType() != EWireType::Tuple) {
            THROW_ERROR_EXCEPTION("%Qlv has unexpected child: %Qv expected: %Qv",
                EWireType::RepeatedVariant8,
                tupleSchema->GetWireType(),
                EWireType::Tuple);
        }

        if (tupleSchema->GetChildren().size() != 2) {
            THROW_ERROR_EXCEPTION("%Qlv has unexpected children count: %Qv expected children count: %Qv",
                EWireType::Tuple,
                skiffSchema->GetChildren().size(),
                1);
        }
        return {
            {descriptor.DictKey(), tupleSchema->GetChildren()[0]},
            {descriptor.DictValue(), tupleSchema->GetChildren()[1]}
        };
    } catch (const std::exception & ex) {
        RethrowCannotMatchField(descriptor, skiffSchema, ex);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <EWireType wireType>
class TSimpleYsonToSkiffConverter
{
public:
    TSimpleYsonToSkiffConverter(TComplexTypeFieldDescriptor descriptor)
        : Descriptor_(std::move(descriptor))
    { }

    void operator () (TYsonPullParserCursor* cursor, TCheckedInDebugSkiffWriter* writer)
    {
        if constexpr (wireType == EWireType::Yson32) {
            TmpString_.clear();
            {
                TStringOutput output(TmpString_);
                TBufferedBinaryYsonWriter ysonWriter(&output);
                cursor->TransferComplexValue(&ysonWriter);
                ysonWriter.Flush();
            }
            writer->WriteYson32(TmpString_);
        } else {
            constexpr auto expectedValueType = WireTypeToYsonItemType<wireType>();
            auto ysonItem = cursor->GetCurrent();
            if (ysonItem.GetType() != expectedValueType) {
                ThrowYsonToSkiffConversionError(Descriptor_, "Unexpected yson type, expected: %Qlv found %Qlv",
                    expectedValueType,
                    ysonItem.GetType());
            }

            if constexpr (wireType == EWireType::Int64) {
                writer->WriteInt64(ysonItem.UncheckedAsInt64());
            } else if constexpr (wireType == EWireType::Uint64) {
                writer->WriteUint64(ysonItem.UncheckedAsUint64());
            } else if constexpr (wireType == EWireType::Boolean) {
                writer->WriteBoolean(ysonItem.UncheckedAsBoolean());
            } else if constexpr (wireType == EWireType::Double) {
                writer->WriteDouble(ysonItem.UncheckedAsDouble());
            } else if constexpr (wireType == EWireType::String32) {
                writer->WriteString32(ysonItem.UncheckedAsString());
            } else if constexpr (wireType == EWireType::Nothing) {
                // do nothing
            } else {
                static_assert(wireType == EWireType::Int64);
            }
            cursor->Next();
        }
    }

private:
    TComplexTypeFieldDescriptor Descriptor_;
    TString TmpString_;
};

TYsonToSkiffConverter CreateSimpleYsonToSkiffConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TYsonToSkiffConverterConfig& config)
{
    const auto& logicalType = descriptor.GetType()->AsSimpleTypeRef();
    const auto valueType = logicalType.GetElement();
    const auto wireType = skiffSchema->GetWireType();
    const auto expectedWireType = GetSkiffTypeForSimpleLogicalType(valueType);
    if (expectedWireType != wireType) {
        // To be consistent we throw catch and rethrow exception to get nesting error.
        try {
            ThrowBadWireType(expectedWireType, wireType);
        } catch (const std::exception& ex) {
            RethrowCannotMatchField(descriptor, skiffSchema, ex);
        }
    }
    switch (wireType) {
#define CASE(x) case x: return TSimpleYsonToSkiffConverter<x>(std::move(descriptor));
        CASE(EWireType::Int64)
        CASE(EWireType::Uint64)
        CASE(EWireType::Boolean)
        CASE(EWireType::Double)
        CASE(EWireType::String32)
        CASE(EWireType::Yson32)
        CASE(EWireType::Nothing)
#undef CASE
        default:
            YT_ABORT();
    }
}

class TOptionalYsonToSkiffConverterImpl
{
public:
    TOptionalYsonToSkiffConverterImpl(
        TYsonToSkiffConverter innerConverter,
        TComplexTypeFieldDescriptor descriptor,
        int ysonOptionalLevel,
        int skiffOptionalLevel)
        : InnerConverter_(std::move(innerConverter))
        , Descriptor_(std::move(descriptor))
        , OuterExpectFilledLevel_(ysonOptionalLevel > 1 ? ysonOptionalLevel - skiffOptionalLevel : 0)
        , OuterTranslateLevel_(ysonOptionalLevel - 1)
        , InnerOptionalTranslate_(skiffOptionalLevel > 0)
    {}

    void operator () (TYsonPullParserCursor* cursor, TCheckedInDebugSkiffWriter* writer)
    {
        auto throwValueExpectedToBeNonempty = [&] {
            ThrowYsonToSkiffConversionError(Descriptor_, "\"#\" found while value expected to be nonempty");
        };

        int outerOptionalsFound = 0;
        for (; outerOptionalsFound < OuterExpectFilledLevel_; ++outerOptionalsFound) {
            if (cursor->GetCurrent().GetType() == EYsonItemType::BeginList) {
                cursor->Next();
            } else if (cursor->GetCurrent().GetType() == EYsonItemType::EntityValue) {
                throwValueExpectedToBeNonempty();
            } else {
                ThrowBadYsonToken(
                    Descriptor_,
                    {EYsonItemType::BeginList},
                    cursor->GetCurrent().GetType());
            }
        }

        for (; outerOptionalsFound < OuterTranslateLevel_; ++outerOptionalsFound) {
            if (cursor->GetCurrent().GetType() == EYsonItemType::BeginList) {
                writer->WriteVariant8Tag(1);
                cursor->Next();
            } else if (cursor->GetCurrent().GetType() == EYsonItemType::EntityValue) {
                writer->WriteVariant8Tag(0);
                cursor->Next();
                goto skip_end_list_tokens;
            } else {
                ThrowBadYsonToken(
                    Descriptor_,
                    {EYsonItemType::BeginList, EYsonItemType::EntityValue},
                    cursor->GetCurrent().GetType());
            }
        }

        if (cursor->GetCurrent().GetType() == EYsonItemType::EntityValue) {
            if (InnerOptionalTranslate_) {
                writer->WriteVariant8Tag(0);
            } else {
                throwValueExpectedToBeNonempty();
            }
            cursor->Next();
        } else {
            if (InnerOptionalTranslate_) {
                writer->WriteVariant8Tag(1);
            }
            InnerConverter_(cursor, writer);
        }

skip_end_list_tokens:
        for (int i = 0; i < outerOptionalsFound; ++i) {
            if (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
                ThrowBadYsonToken(Descriptor_, {EYsonItemType::EndList}, cursor->GetCurrent().GetType());
            }
            cursor->Next();
        }
    }

private:
    const TYsonToSkiffConverter InnerConverter_;
    const TComplexTypeFieldDescriptor Descriptor_;

    // Max level of outer yson optional we expect to be filled.
    const int OuterExpectFilledLevel_;
    // Max level of outer optional we want to translate to yson.
    const int OuterTranslateLevel_;

    // If true we translate inner yson optional into skiff optional
    // if false we expect inner yson optional to be filled.
    const bool InnerOptionalTranslate_;
};

class TOptionalNullYsonToSkiffConverterImpl
{
public:
    TOptionalNullYsonToSkiffConverterImpl(
        TComplexTypeFieldDescriptor descriptor,
        int ysonOptionalLevel,
        int skiffOptionalLevel)
        : Descriptor_(std::move(descriptor))
        , OuterExpectFilledLevel_(ysonOptionalLevel - skiffOptionalLevel)
        , OuterTranslateLevel_(ysonOptionalLevel)
    {}

    void operator () (TYsonPullParserCursor* cursor, TCheckedInDebugSkiffWriter* writer)
    {
        auto throwValueExpectedToBeNonempty = [&] {
            ThrowYsonToSkiffConversionError(Descriptor_, "\"#\" found while value expected to be nonempty");
        };

        int outerOptionalsFound = 0;
        for (; outerOptionalsFound < OuterExpectFilledLevel_; ++outerOptionalsFound) {
            if (cursor->GetCurrent().GetType() == EYsonItemType::BeginList) {
                cursor->Next();
            } else if (cursor->GetCurrent().GetType() == EYsonItemType::EntityValue) {
                throwValueExpectedToBeNonempty();
            } else {
                ThrowBadYsonToken(
                    Descriptor_,
                    {EYsonItemType::BeginList},
                    cursor->GetCurrent().GetType());
            }
        }

        for (; outerOptionalsFound < OuterTranslateLevel_; ++outerOptionalsFound) {
            if (cursor->GetCurrent().GetType() == EYsonItemType::BeginList) {
                writer->WriteVariant8Tag(1);
                cursor->Next();
            } else if (cursor->GetCurrent().GetType() == EYsonItemType::EntityValue) {
                writer->WriteVariant8Tag(0);
                cursor->Next();
                goto skip_end_list_tokens;
            } else {
                ThrowBadYsonToken(
                    Descriptor_,
                    {EYsonItemType::BeginList, EYsonItemType::EntityValue},
                    cursor->GetCurrent().GetType());
            }
        }

        if (cursor->GetCurrent().GetType() != EYsonItemType::EntityValue) {
            ThrowBadYsonToken(
                Descriptor_,
                {EYsonItemType::EntityValue},
                cursor->GetCurrent().GetType());
        }
        cursor->Next();

skip_end_list_tokens:
        for (int i = 0; i < outerOptionalsFound; ++i) {
            if (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
                ThrowBadYsonToken(Descriptor_, {EYsonItemType::EndList}, cursor->GetCurrent().GetType());
            }
            cursor->Next();
        }
    }

private:
    const TComplexTypeFieldDescriptor Descriptor_;

    // How many levels of yson optional we expect to be filled.
    const int OuterExpectFilledLevel_;
    // How many levels of outer optional we want to translate to yson
    const int OuterTranslateLevel_;
};

TYsonToSkiffConverter CreateOptionalYsonToSkiffConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TYsonToSkiffConverterConfig& config)
{
    const bool allowOmitOptional = config.AllowOmitTopLevelOptional && context.NestingLevel == 0;
    auto match = MatchOptionalTypes(
        descriptor,
        skiffSchema,
        allowOmitOptional);

    if (match.InnerTypes.first.GetType()->IsNullable()) {
        return TOptionalNullYsonToSkiffConverterImpl(
            descriptor,
            match.LogicalNesting,
            match.SkiffNesting);
    } else {
        auto innerConverter = CreateYsonToSkiffConverterImpl(
            std::move(match.InnerTypes.first),
            match.InnerTypes.second,
            context,
            config);

        return TOptionalYsonToSkiffConverterImpl(
            innerConverter,
            std::move(descriptor),
            match.LogicalNesting,
            match.SkiffNesting);
    }
}

TYsonToSkiffConverter CreateListYsonToSkiffConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TYsonToSkiffConverterConfig& config)
{
    auto match = MatchListTypes(descriptor, skiffSchema);
    auto innerConverter = CreateYsonToSkiffConverterImpl(
        std::move(match.first),
        match.second,
        context,
        config);
    return [innerConverter = innerConverter, descriptor = std::move(descriptor)] (
        TYsonPullParserCursor* cursor,
        TCheckedInDebugSkiffWriter* writer)
    {
        if (cursor->GetCurrent().GetType() != EYsonItemType::BeginList) {
            ThrowBadYsonToken(descriptor, {EYsonItemType::BeginList}, cursor->GetCurrent().GetType());
        }
        cursor->Next();

        while (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
            writer->WriteVariant8Tag(0);
            innerConverter(cursor, writer);
        }
        writer->WriteVariant8Tag(EndOfSequenceTag<ui8>());
        cursor->Next();
    };
}

class TInfiniteEntity
{
public:
    TInfiniteEntity()
        : Stream_(AsStringBuf("#;#;#;#;#;#;#;#;"))
        , Parser_(&Stream_, EYsonType::ListFragment)
        , Cursor_(&Parser_)
    {}

    TYsonPullParserCursor* GetCursor()
    {
        return &Cursor_;
    }

private:
    class TRingBufferStream
        : public IZeroCopyInput
    {
    public:
        explicit TRingBufferStream(TStringBuf buffer)
            : Buffer_(buffer)
            , Pointer_(Buffer_.data())
        {}

    private:
        virtual size_t DoNext(const void** ptr, size_t len) override
        {
            const auto end = Buffer_.data() + Buffer_.size();
            auto result = Min<size_t>(len, end - Pointer_);
            *ptr = Pointer_;
            Pointer_ += result;
            if (Pointer_ == end) {
                Pointer_ = Buffer_.data();
            }
            return result;
        }

    private:
        const TStringBuf Buffer_;
        const char* Pointer_;
    };

private:
    TRingBufferStream Stream_;
    TYsonPullParser Parser_;
    TYsonPullParserCursor Cursor_;
};

TYsonToSkiffConverter CreateStructYsonToSkiffConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TYsonToSkiffConverterConfig& config)
{
    TYsonToSkiffConverter skipYsonValue = [](TYsonPullParserCursor* cursor, TCheckedInDebugSkiffWriter* /*writer*/) {
        cursor->SkipComplexValue();
    };

    auto fieldMatchList = MatchStructTypes(descriptor, skiffSchema);
    std::vector<TYsonToSkiffConverter> converterList;
    for (const auto&[fieldDescriptor, fieldSkiffSchema] : fieldMatchList) {
        if (fieldSkiffSchema) {
            auto converter = CreateYsonToSkiffConverterImpl(fieldDescriptor, fieldSkiffSchema, context, config);
            converterList.emplace_back(converter);
        } else {
            converterList.emplace_back(skipYsonValue);
        }
    }

    return [converterList = std::move(converterList), descriptor = std::move(descriptor)]
        (TYsonPullParserCursor* cursor, TCheckedInDebugSkiffWriter* writer) {
        if (cursor->GetCurrent().GetType() != EYsonItemType::BeginList) {
            ThrowBadYsonToken(descriptor, {EYsonItemType::BeginList}, cursor->GetCurrent().GetType());
        }
        cursor->Next();
        for (auto it = converterList.begin(), end = converterList.end(); it != end; ++it) {
            if (cursor->GetCurrent().GetType() == EYsonItemType::EndList) {
                TInfiniteEntity infiniteEntity;
                auto entityCursor = infiniteEntity.GetCursor();
                do {
                    const auto& converter = *it;
                    converter(entityCursor, writer);
                    ++it;
                } while (it != end);
                break;
            }
            const auto& converter = *it;
            converter(cursor, writer);
        }

        if (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
            ThrowBadYsonToken(descriptor, {EYsonItemType::EndList}, cursor->GetCurrent().GetType());
        }
        cursor->Next();
    };
}

TYsonToSkiffConverter CreateTupleYsonToSkiffConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TYsonToSkiffConverterConfig& config)
{
    auto tupleMatch = MatchTupleTypes(descriptor, skiffSchema);
    std::vector<TYsonToSkiffConverter> converterList;
    for (const auto&[descriptor, skiffSchema] : tupleMatch) {
        auto converter = CreateYsonToSkiffConverterImpl(descriptor, skiffSchema, context, config);
        converterList.emplace_back(converter);
    }

    return [converterList = std::move(converterList), descriptor = std::move(descriptor)]
        (TYsonPullParserCursor* cursor, TCheckedInDebugSkiffWriter* writer) {
        if (cursor->GetCurrent().GetType() != EYsonItemType::BeginList) {
            ThrowBadYsonToken(descriptor, {EYsonItemType::BeginList}, cursor->GetCurrent().GetType());
        }
        cursor->Next();
        for (const auto& converter : converterList) {
            converter(cursor, writer);
        }

        if (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
            ThrowBadYsonToken(descriptor, {EYsonItemType::EndList}, cursor->GetCurrent().GetType());
        }
        cursor->Next();
    };
}

template <EWireType wireType>
class TVariantYsonToSkiffConverterImpl
{
public:
    TVariantYsonToSkiffConverterImpl(std::vector<TYsonToSkiffConverter> converterList, TComplexTypeFieldDescriptor descriptor)
        : ConverterList_(std::move(converterList))
        , Descriptor_(std::move(descriptor))
    { }

    void operator () (TYsonPullParserCursor* cursor, TCheckedInDebugSkiffWriter* writer)
    {
        if (cursor->GetCurrent().GetType() != EYsonItemType::BeginList) {
            ThrowBadYsonToken(Descriptor_, {EYsonItemType::BeginList}, cursor->GetCurrent().GetType());
        }
        cursor->Next();
        if (cursor->GetCurrent().GetType() != EYsonItemType::Int64Value) {
            ThrowBadYsonToken(Descriptor_, {EYsonItemType::Int64Value}, cursor->GetCurrent().GetType());
        }
        auto tag = cursor->GetCurrent().UncheckedAsInt64();
        cursor->Next();
        if (tag >= ConverterList_.size()) {
            ThrowYsonToSkiffConversionError(Descriptor_, "variant tag (%v) exceeds %v children count (%v)",
                tag,
                wireType,
                ConverterList_.size());
        }
        if constexpr (wireType == EWireType::Variant8) {
            writer->WriteVariant8Tag(tag);
        } else {
            static_assert(wireType == EWireType::Variant16);
            writer->WriteVariant16Tag(tag);
        }
        ConverterList_[tag](cursor, writer);
        if (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
            ThrowBadYsonToken(Descriptor_, {EYsonItemType::EndList}, cursor->GetCurrent().GetType());
        }
        cursor->Next();
    }

private:
    const std::vector<TYsonToSkiffConverter> ConverterList_;
    const TComplexTypeFieldDescriptor Descriptor_;
};

TYsonToSkiffConverter CreateVariantYsonToSkiffConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TYsonToSkiffConverterConfig& config)
{
    std::vector<TTypePair> variantMatch;
    if (descriptor.GetType()->GetMetatype() == ELogicalMetatype::VariantStruct) {
        variantMatch = MatchVariantStructTypes(descriptor, skiffSchema);
    } else {
        YT_VERIFY(descriptor.GetType()->GetMetatype() == ELogicalMetatype::VariantTuple);
        variantMatch = MatchVariantTupleTypes(descriptor, skiffSchema);
    }

    std::vector<TYsonToSkiffConverter> converterList;
    for (const auto&[descriptor, skiffSchema] : variantMatch) {
        auto converter = CreateYsonToSkiffConverterImpl(descriptor, skiffSchema, context, config);
        converterList.emplace_back(converter);
    }

    if (skiffSchema->GetWireType() == EWireType::Variant8) {
        return TVariantYsonToSkiffConverterImpl<EWireType::Variant8>(std::move(converterList), std::move(descriptor));
    } else if (skiffSchema->GetWireType() == EWireType::Variant16) {
        return TVariantYsonToSkiffConverterImpl<EWireType::Variant16>(std::move(converterList), std::move(descriptor));
    }
    Y_UNREACHABLE();
}

TYsonToSkiffConverter CreateDictYsonToSkiffConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TYsonToSkiffConverterConfig& config)
{
    const auto [keyMatch, valueMatch] = MatchDictTypes(descriptor, skiffSchema);
    auto keyConverter = CreateYsonToSkiffConverterImpl(keyMatch.first, keyMatch.second, context, config);
    auto valueConverter = CreateYsonToSkiffConverterImpl(valueMatch.first, valueMatch.second, context, config);

    return [
        keyConverter = std::move(keyConverter),
        valueConverter = std::move(valueConverter),
        descriptor = std::move(descriptor)
    ] (TYsonPullParserCursor* cursor, TCheckedInDebugSkiffWriter* writer) {
        if (cursor->GetCurrent().GetType() != EYsonItemType::BeginList) {
            ThrowBadYsonToken(descriptor, {EYsonItemType::BeginList}, cursor->GetCurrent().GetType());
        }
        cursor->Next();
        while (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
            writer->WriteVariant8Tag(0);
            if (cursor->GetCurrent().GetType() != EYsonItemType::BeginList) {
                ThrowBadYsonToken(descriptor, {EYsonItemType::BeginList}, cursor->GetCurrent().GetType());
            }
            cursor->Next();
            keyConverter(cursor, writer);
            valueConverter(cursor, writer);
            if (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
                ThrowBadYsonToken(descriptor, {EYsonItemType::EndList}, cursor->GetCurrent().GetType());
            }
            cursor->Next();
        }
        writer->WriteVariant8Tag(EndOfSequenceTag<ui8>());
        cursor->Next(); // Skip EYsonItemType::EndList.
    };
}

TYsonToSkiffConverter CreateYsonToSkiffConverterImpl(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TYsonToSkiffConverterConfig& config)
{
    TConverterCreationContext innerContext = context;
    ++innerContext.NestingLevel;
    const auto& logicalType = descriptor.GetType();
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CreateSimpleYsonToSkiffConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::Optional:
            return CreateOptionalYsonToSkiffConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::List:
            return CreateListYsonToSkiffConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::Struct:
            return CreateStructYsonToSkiffConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::Tuple:
            return CreateTupleYsonToSkiffConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::VariantTuple:
            return CreateVariantYsonToSkiffConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::VariantStruct:
            return CreateVariantYsonToSkiffConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::Dict:
            return CreateDictYsonToSkiffConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::Tagged:
            // We have detagged our type previously.
            YT_ABORT();
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

template<EWireType wireType>
class TSimpleSkiffToYsonConverter
{
public:
    explicit TSimpleSkiffToYsonConverter(TComplexTypeFieldDescriptor descriptor)
        : Descriptor_(std::move(descriptor))
    {}

    Y_FORCE_INLINE void operator () (TCheckedInDebugSkiffParser* parser, TCheckedInDebugYsonTokenWriter* writer)
    {
        if constexpr (wireType == EWireType::Int64) {
            writer->WriteBinaryInt64(parser->ParseInt64());
        } else if constexpr (wireType == EWireType::Uint64) {
            writer->WriteBinaryUint64(parser->ParseUint64());
        } else if constexpr (wireType == EWireType::Boolean) {
            writer->WriteBinaryBoolean(parser->ParseBoolean());
        } else if constexpr (wireType == EWireType::Double) {
            writer->WriteBinaryDouble(parser->ParseDouble());
        } else if constexpr (wireType == EWireType::String32) {
            writer->WriteBinaryString(parser->ParseString32());
        } else if constexpr (wireType == EWireType::Yson32) {
            TMemoryInput inputStream(parser->ParseYson32());
            TYsonPullParser pullParser(&inputStream, EYsonType::Node);
            TYsonPullParserCursor(&pullParser).TransferComplexValue(writer);
        } else if constexpr (wireType == EWireType::Nothing) {
            writer->WriteEntity();
        } else {
            static_assert(wireType == EWireType::Int64);
        }
    }

private:
    TComplexTypeFieldDescriptor Descriptor_;
};

TSkiffToYsonConverter CreateSimpleSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TSkiffToYsonConverterConfig& config)
{
    const auto& logicalType = descriptor.GetType()->AsSimpleTypeRef();
    auto valueType = logicalType.GetElement();
    auto wireType = skiffSchema->GetWireType();
    if (GetSkiffTypeForSimpleLogicalType(valueType) != wireType) {
        THROW_ERROR_EXCEPTION("Field %Qv of type %Qv cannot be represented as skiff type %Qlv",
            descriptor.GetDescription(),
            *descriptor.GetType(),
            wireType
        );
    }

    switch (wireType) {
#define CASE(x) case x: return TSimpleSkiffToYsonConverter<x>(std::move(descriptor));
        CASE(EWireType::Int64)
        CASE(EWireType::Uint64)
        CASE(EWireType::Boolean)
        CASE(EWireType::Double)
        CASE(EWireType::String32)
        CASE(EWireType::Yson32)
        CASE(EWireType::Nothing)
#undef CASE
        default:
            YT_ABORT();
    }
}

class TOptionalSkiffToYsonConverterImpl
{
public:
    TOptionalSkiffToYsonConverterImpl(
        TSkiffToYsonConverter innerConverter,
        TComplexTypeFieldDescriptor descriptor,
        int ysonNesting,
        int skiffNesting)
        : InnerConverter_(std::move(innerConverter))
        , Descriptor_(std::move(descriptor))
        , OuterFill_(ysonNesting > 1 ? ysonNesting - skiffNesting : 0)
        , OuterTranslate_(ysonNesting - OuterFill_ - 1)
        , InnerTranslate_(skiffNesting > 0)
    {
        YT_VERIFY(skiffNesting >= 0);
        YT_VERIFY(ysonNesting > 0);

        YT_VERIFY(skiffNesting <= ysonNesting);
        YT_VERIFY(ysonNesting <= skiffNesting + 1);
    }

    void operator () (TCheckedInDebugSkiffParser* parser, TCheckedInDebugYsonTokenWriter* writer)
    {
        for (int i = 0; i < OuterFill_; ++i) {
            writer->WriteBeginList();
        }

        int outerOptionalsFilled = 0;
        for (; outerOptionalsFilled < OuterTranslate_; ++outerOptionalsFilled) {
            auto tag = parser->ParseVariant8Tag();
            if (tag == 0) {
                writer->WriteEntity();
                goto write_list_ends;
            } else if (tag == 1) {
                writer->WriteBeginList();
            } else {
                ThrowUnexpectedVariant8Tag(tag);
            }
        }

        if (InnerTranslate_) {
            auto tag = parser->ParseVariant8Tag();
            if (tag == 0) {
                writer->WriteEntity();
                goto write_list_ends;
            } else if (tag != 1) {
                ThrowUnexpectedVariant8Tag(tag);
            }
        }
        InnerConverter_(parser, writer);

write_list_ends:
        const int toClose = outerOptionalsFilled + OuterFill_;
        for (int i = 0; i < toClose; ++i) {
            writer->WriteEndList();
        }
    }

private:
    void ThrowUnexpectedVariant8Tag(ui8 tag) const
    {
        ThrowSkiffToYsonConversionError(Descriptor_, "Unexpected %lv tag, expected %Qv or %Qv got %Qv",
            EWireType::Variant8,
            0,
            1,
            tag);
    }

private:
    const TSkiffToYsonConverter InnerConverter_;
    const TComplexTypeFieldDescriptor Descriptor_;

    // How many levels of yson optional we set unconditionally.
    const int OuterFill_;

    // How many levels of skiff optionals we translate to yson outer optionals (which are encoded as list).
    const int OuterTranslate_;

    const bool InnerTranslate_;
};

class TOptionalNullSkiffToYsonConverterImpl
{
public:
    TOptionalNullSkiffToYsonConverterImpl(
        TComplexTypeFieldDescriptor descriptor,
        int ysonNesting,
        int skiffNesting)
        : Descriptor_(std::move(descriptor))
        , OuterFill_(ysonNesting > 1 ? ysonNesting - skiffNesting : 0)
        , OuterTranslate_(ysonNesting - OuterFill_)
    {
        YT_VERIFY(skiffNesting >= 0);
        YT_VERIFY(ysonNesting > 0);

        YT_VERIFY(skiffNesting <= ysonNesting);
        YT_VERIFY(ysonNesting <= skiffNesting + 1);
    }

    void operator () (TCheckedInDebugSkiffParser* parser, TCheckedInDebugYsonTokenWriter* writer)
    {
        for (int i = 0; i < OuterFill_; ++i) {
            writer->WriteBeginList();
        }

        int outerOptionalsFilled = 0;
        for (; outerOptionalsFilled < OuterTranslate_; ++outerOptionalsFilled) {
            auto tag = parser->ParseVariant8Tag();
            if (tag == 0) {
                writer->WriteEntity();
                goto write_list_ends;
            } else if (tag == 1) {
                writer->WriteBeginList();
            } else {
                ThrowUnexpectedVariant8Tag(tag);
            }
        }
        writer->WriteEntity();

write_list_ends:
        const int toClose = outerOptionalsFilled + OuterFill_;
        for (int i = 0; i < toClose; ++i) {
            writer->WriteEndList();
        }
    }

private:
    void ThrowUnexpectedVariant8Tag(ui8 tag) const
    {
        ThrowSkiffToYsonConversionError(Descriptor_, "Unexpected %lv tag, expected %Qv or %Qv got %Qv",
            EWireType::Variant8,
            0,
            1,
            tag);
    }

private:
    const TSkiffToYsonConverter InnerConverter_;
    const TComplexTypeFieldDescriptor Descriptor_;

    // How many levels of yson optional we set unconditionally.
    const int OuterFill_;

    // How many levels of skiff optionals we translate to yson outer optionals (which are encoded as list).
    const int OuterTranslate_;
};

TSkiffToYsonConverter CreateOptionalSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TSkiffToYsonConverterConfig& config)
{
    const bool allowOmitOptional = config.AllowOmitTopLevelOptional && context.NestingLevel == 0;
    auto match = MatchOptionalTypes(descriptor, skiffSchema, allowOmitOptional);
    if (match.LogicalNesting != match.SkiffNesting) {
        if (!config.AllowOmitTopLevelOptional || context.NestingLevel > 0) {
            RethrowCannotMatchField(descriptor, skiffSchema, TErrorException()
                <<= TError("Optional nesting mismatch"));
        }
    }

    if (match.InnerTypes.first.GetType()->IsNullable()) {
        return TOptionalNullSkiffToYsonConverterImpl(std::move(descriptor), match.LogicalNesting, match.SkiffNesting);
    } else {
        auto innerConverter = CreateSkiffToYsonConverterImpl(
            std::move(match.InnerTypes.first),
            match.InnerTypes.second,
            context,
            config);
        return TOptionalSkiffToYsonConverterImpl(
            innerConverter, std::move(descriptor), match.LogicalNesting, match.SkiffNesting);
    }
}

TSkiffToYsonConverter CreateListSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TSkiffToYsonConverterConfig& config)
{
    auto match = MatchListTypes(descriptor, skiffSchema);
    auto innerConverter = CreateSkiffToYsonConverterImpl(std::move(match.first), match.second, context, config);

    return [innerConverter = innerConverter, descriptor=std::move(descriptor)](TCheckedInDebugSkiffParser* parser, TCheckedInDebugYsonTokenWriter* writer) {
        writer->WriteBeginList();
        while (true) {
            auto tag = parser->ParseVariant8Tag();
            if (tag == EndOfSequenceTag<ui8>()) {
                break;
            } else if (tag != 0) {
                ThrowSkiffToYsonConversionError(descriptor, "Unexpected %lv tag, expected %Qv or %Qv got %Qv",
                    EWireType::RepeatedVariant8,
                    0,
                    EndOfSequenceTag<ui8>(),
                    tag);
            }
            innerConverter(parser, writer);
            writer->WriteItemSeparator();
        }
        writer->WriteEndList();
    };
}

TSkiffToYsonConverter CreateStructSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TSkiffToYsonConverterConfig& config)
{
    const auto insertEntity = [](TCheckedInDebugSkiffParser* /*parser*/, TCheckedInDebugYsonTokenWriter* writer) {
        writer->WriteEntity();
    };

    auto structMatch = MatchStructTypes(descriptor, skiffSchema);
    std::vector<TSkiffToYsonConverter> converterList;
    for (const auto& [fieldDescriptor, fieldSkiffSchema] : structMatch) {
        if (fieldSkiffSchema) {
            converterList.emplace_back(CreateSkiffToYsonConverterImpl(fieldDescriptor, fieldSkiffSchema, context, config));
        } else if (fieldDescriptor.GetType()->GetMetatype() == ELogicalMetatype::Optional) {
            converterList.emplace_back(insertEntity);
        } else {
            RethrowCannotMatchField(
                descriptor,
                skiffSchema,
                TErrorException() <<= TError(
                    "Non optional struct field %Qv is missing in skiff schema",
                    fieldDescriptor.GetDescription()));
        }
    }

    return [converterList = converterList](TCheckedInDebugSkiffParser* parser, TCheckedInDebugYsonTokenWriter* writer) {
        writer->WriteBeginList();
        for (const auto& converter : converterList) {
            converter(parser, writer);
            writer->WriteItemSeparator();
        }
        writer->WriteEndList();
    };
}

TSkiffToYsonConverter CreateTupleSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TSkiffToYsonConverterConfig& config)
{
    auto tupleMatch = MatchTupleTypes(descriptor, skiffSchema);
    std::vector<TSkiffToYsonConverter> converterList;
    for (const auto& [fieldDescriptor, fieldSkiffSchema] : tupleMatch) {
        converterList.emplace_back(CreateSkiffToYsonConverterImpl(fieldDescriptor, fieldSkiffSchema, context, config));
    }
    return [converterList = converterList](TCheckedInDebugSkiffParser* parser, TCheckedInDebugYsonTokenWriter* writer) {
        writer->WriteBeginList();
        for (const auto& converter : converterList) {
            converter(parser, writer);
            writer->WriteItemSeparator();
        }
        writer->WriteEndList();
    };
}

template <EWireType wireType>
class TVariantSkiffToYsonConverterImpl
{
public:
    TVariantSkiffToYsonConverterImpl(std::vector<TSkiffToYsonConverter> converterList, TComplexTypeFieldDescriptor descriptor)
        : ConverterList_(std::move(converterList))
        , Descriptor_(std::move(descriptor))
    { }

    void operator () (TCheckedInDebugSkiffParser* parser, TCheckedInDebugYsonTokenWriter* writer)
    {
        int tag;
        if constexpr (wireType == EWireType::Variant8) {
            tag = parser->ParseVariant8Tag();
        } else {
            static_assert(wireType == EWireType::Variant16);
            tag = parser->ParseVariant16Tag();
        }

        if (tag >= ConverterList_.size()) {
            ThrowSkiffToYsonConversionError(Descriptor_, "Variant tag (%v) exceeds %v children count (%v)",
                tag,
                wireType,
                ConverterList_.size());
        }
        writer->WriteBeginList();
        writer->WriteBinaryInt64(tag);
        writer->WriteItemSeparator();
        ConverterList_[tag](parser, writer);
        writer->WriteItemSeparator();
        writer->WriteEndList();
    }

private:
    const std::vector<TSkiffToYsonConverter> ConverterList_;
    const TComplexTypeFieldDescriptor Descriptor_;
};

TSkiffToYsonConverter CreateVariantSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TSkiffToYsonConverterConfig& config)
{
    std::vector<TTypePair> variantMatch;
    if (descriptor.GetType()->GetMetatype() == ELogicalMetatype::VariantStruct) {
        variantMatch = MatchVariantStructTypes(descriptor, skiffSchema);
    } else {
        YT_VERIFY(descriptor.GetType()->GetMetatype() == ELogicalMetatype::VariantTuple);
        variantMatch = MatchVariantTupleTypes(descriptor, skiffSchema);
    }

    std::vector<TSkiffToYsonConverter> converterList;
    for (const auto& [fieldDescriptor, fieldSkiffSchema] : variantMatch) {
        converterList.emplace_back(CreateSkiffToYsonConverterImpl(fieldDescriptor, fieldSkiffSchema, context, config));
    }

    if (skiffSchema->GetWireType() == EWireType::Variant8) {
        return TVariantSkiffToYsonConverterImpl<EWireType::Variant8>(std::move(converterList), std::move(descriptor));
    } else {
        YT_VERIFY(skiffSchema->GetWireType() == EWireType::Variant16);
        return TVariantSkiffToYsonConverterImpl<EWireType::Variant16>(std::move(converterList), std::move(descriptor));
    }
}

TSkiffToYsonConverter CreateDictSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TSkiffToYsonConverterConfig& config)
{
    auto [keyMatch, valueMatch] = MatchDictTypes(descriptor, skiffSchema);
    auto keyConverter = CreateSkiffToYsonConverterImpl(std::move(keyMatch.first), std::move(keyMatch.second), context, config);
    auto valueConverter = CreateSkiffToYsonConverterImpl(std::move(valueMatch.first), std::move(valueMatch.second), context, config);

    return [
        keyConverter = std::move(keyConverter),
        valueConverter = std::move(valueConverter),
        descriptor=std::move(descriptor)
    ] (TCheckedInDebugSkiffParser* parser, TCheckedInDebugYsonTokenWriter* writer) {
        writer->WriteBeginList();
        while (true) {
            auto tag = parser->ParseVariant8Tag();
            if (tag == EndOfSequenceTag<ui8>()) {
                break;
            } else if (tag != 0) {
                ThrowSkiffToYsonConversionError(descriptor, "Unexpected %lv tag, expected %Qv or %Qv got %Qv",
                    EWireType::RepeatedVariant8,
                    0,
                    EndOfSequenceTag<ui8>(),
                    tag);
            }
            writer->WriteBeginList();
            {
                keyConverter(parser, writer);
                writer->WriteItemSeparator();
                valueConverter(parser, writer);
                writer->WriteItemSeparator();
            }
            writer->WriteEndList();
            writer->WriteItemSeparator();
        }
        writer->WriteEndList();
    };
}

TSkiffToYsonConverter CreateSkiffToYsonConverterImpl(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TConverterCreationContext& context,
    const TSkiffToYsonConverterConfig& config)
{
    TConverterCreationContext innerContext = context;
    ++innerContext.NestingLevel;
    const auto& logicalType = descriptor.GetType();
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CreateSimpleSkiffToYsonConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::Optional:
            return CreateOptionalSkiffToYsonConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::List:
            return CreateListSkiffToYsonConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::Struct:
            return CreateStructSkiffToYsonConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::Tuple:
            return CreateTupleSkiffToYsonConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::VariantStruct:
            return CreateVariantSkiffToYsonConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::VariantTuple:
            return CreateVariantSkiffToYsonConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::Dict:
            return CreateDictSkiffToYsonConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::Tagged:
            // We have detagged our type previously.
            YT_ABORT();
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYsonToSkiffConverter CreateYsonToSkiffConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TYsonToSkiffConverterConfig& config)
{
    TConverterCreationContext context;
    // CreateYsonToSkiffConverterImpl will increment NestingLevel to 0 for the top level element.
    context.NestingLevel = -1;
    return CreateYsonToSkiffConverterImpl(descriptor.Detag(), skiffSchema, context, config);
}

TSkiffToYsonConverter CreateSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TSkiffToYsonConverterConfig& config)
{
    TConverterCreationContext context;
    context.NestingLevel = -1;
    return CreateSkiffToYsonConverterImpl(descriptor.Detag(), skiffSchema, context, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
