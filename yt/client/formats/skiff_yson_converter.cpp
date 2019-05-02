#include "skiff_yson_converter.h"

#include <yt/client/table_client/logical_type.h>

#include <yt/core/skiff/skiff.h>
#include <yt/core/skiff/skiff_schema.h>
#include <yt/core/yson/pull_parser.h>
#include <yt/core/yson/parser.h>

#include <util/stream/zerocopy.h>

namespace NYT::NFormats {

using namespace NSkiff;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using TTypePair = std::pair<TComplexTypeFieldDescriptor, TSkiffSchemaPtr>;

struct TYsonToSkiffCreatorContext;

TYsonToSkiffConverter CreateYsonToSkiffConverterImpl(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TYsonToSkiffCreatorContext& context,
    const TYsonToSkiffConverterConfig& config);

////////////////////////////////////////////////////////////////////////////////

struct TYsonToSkiffCreatorContext
{
    int NestingLevel = 0;
};

constexpr EWireType GetSkiffTypeForSimpleLogicalType(ESimpleLogicalValueType logicalType)
{
    switch (GetPhysicalType(logicalType)) {
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
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::Null:
        case EValueType::TheBottom:
            break;
    }
    Y_UNREACHABLE();
}

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
        YCHECK(expected.size() == 1);
        expectationString << Format("%Qlv", expected[0]);
    }

    THROW_ERROR_EXCEPTION(
        "Yson to Skiff conversion error while converting %Qv field",
        descriptor.GetDescription())
        << TError ("Bad yson token type, expected %v actual: %Qlv",
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

TOptionalTypesMatch MatchOptionalTypes(const TComplexTypeFieldDescriptor& descriptor, const TSkiffSchemaPtr& skiffSchema)
{
    try {
        auto innerDescriptor = descriptor;
        int logicalNesting = 0;
        while (innerDescriptor.GetType()->GetMetatype() == ELogicalMetatype::Optional) {
            ++logicalNesting;
            innerDescriptor = innerDescriptor.OptionalElement();
        }
        YCHECK(logicalNesting);

        int skiffNesting = 0;
        TSkiffSchemaPtr innerSkiffSchema = skiffSchema;
        while (auto child = GetOptionalChild(innerSkiffSchema)) {
            ++skiffNesting;
            innerSkiffSchema = child;
        }

        if (logicalNesting != skiffNesting && logicalNesting != skiffNesting + 1) {
            THROW_ERROR_EXCEPTION("Optional nesting mismatch, logical type nesting: %Qv, skiff nesting: %Qv",
                logicalNesting,
                skiffNesting);
        }

        return {{std::move(innerDescriptor), innerSkiffSchema}, logicalNesting, skiffNesting};
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
            THROW_ERROR_EXCEPTION("%Qv child %Qv is not found in logical typel",
                EWireType::Tuple,
                skiffName);
        }
        return result;
    } catch (const std::exception& ex) {
        RethrowCannotMatchField(descriptor, skiffSchema, ex);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <EWireType wireType>
class TSimpleYsonToSkiffConverter
{
public:
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
                THROW_ERROR_EXCEPTION("Internal error: unexpected yson type, expected: %Qlv found %Qlv",
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
            } else {
                static_assert(wireType == EWireType::Int64);
            }
            cursor->Next();
        }
    }

private:
    TString TmpString_;
};

TYsonToSkiffConverter CreateSimpleYsonToSkiffConverter(
    const TComplexTypeFieldDescriptor& descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TYsonToSkiffCreatorContext& context,
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
#define CASE(x) case x: return TSimpleYsonToSkiffConverter<x>();
        CASE(EWireType::Int64)
        CASE(EWireType::Uint64)
        CASE(EWireType::Boolean)
        CASE(EWireType::Double)
        CASE(EWireType::String32)
        CASE(EWireType::Yson32)
#undef CASE
        default:
            Y_UNREACHABLE();
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
            THROW_ERROR_EXCEPTION("Cannot parse %Qv, \"#\" found while value expected to be nonempty",
                Descriptor_.GetDescription(),
                EYsonItemType::EntityValue);
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

    // How many levels of yson optional we expect to be filled.
    const int OuterExpectFilledLevel_;
    // How many levels of outer optional we want to translate to yson
    const int OuterTranslateLevel_;

    // If true we translate inner yson optional into skiff optional
    // if false we expect inner yson optional to be filled.
    const bool InnerOptionalTranslate_;
};

TYsonToSkiffConverter CreateOptionalYsonToSkiffConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TYsonToSkiffCreatorContext& context,
    const TYsonToSkiffConverterConfig& config)
{
    auto match = MatchOptionalTypes(descriptor, skiffSchema);
    if (match.LogicalNesting != match.SkiffNesting) {
        if (!config.ExpectTopLevelOptionalSet || context.NestingLevel > 0) {
            RethrowCannotMatchField(descriptor, skiffSchema, TErrorException()
                <<= TError("Optional nesting mismatch"));
        }
    }

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

TYsonToSkiffConverter CreateListYsonToSkiffConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TYsonToSkiffCreatorContext& context,
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
    const TYsonToSkiffCreatorContext& context,
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

TYsonToSkiffConverter CreateYsonToSkiffConverterImpl(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TYsonToSkiffCreatorContext& context,
    const TYsonToSkiffConverterConfig& config)
{
    TYsonToSkiffCreatorContext innerContext = context;
    ++innerContext.NestingLevel;
    const auto& logicalType = descriptor.GetType();
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CreateSimpleYsonToSkiffConverter(descriptor, skiffSchema, innerContext, config);
        case ELogicalMetatype::Optional:
            return CreateOptionalYsonToSkiffConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::List:
            return CreateListYsonToSkiffConverter(std::move(descriptor), skiffSchema, innerContext, config);
        case ELogicalMetatype::Struct:
            return CreateStructYsonToSkiffConverter(std::move(descriptor), skiffSchema, innerContext, config);
    }
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

template<EWireType wireType>
class TSimpleSkiffToYsonConverter
{
public:
    explicit TSimpleSkiffToYsonConverter(TComplexTypeFieldDescriptor descriptor)
        : Descriptor_(std::move(descriptor))
    {}

    Y_FORCE_INLINE void operator () (NSkiff::TCheckedInDebugSkiffParser* parser, NYson::IYsonConsumer* consumer)
    {
        if constexpr (wireType == EWireType::Int64) {
            consumer->OnInt64Scalar(parser->ParseInt64());
        } else if constexpr (wireType == EWireType::Uint64) {
            consumer->OnUint64Scalar(parser->ParseUint64());
        } else if constexpr (wireType == EWireType::Boolean) {
            consumer->OnBooleanScalar(parser->ParseBoolean());
        } else if constexpr (wireType == EWireType::Double) {
            consumer->OnDoubleScalar(parser->ParseDouble());
        } else if constexpr (wireType == EWireType::String32) {
            consumer->OnStringScalar(parser->ParseString32());
        } else if constexpr (wireType == EWireType::Yson32) {
            auto yson = parser->ParseYson32();
            ParseYsonStringBuffer(yson, EYsonType::Node, consumer);
        } else {
            static_assert(wireType == EWireType::Int64);
        }
    }

private:
    TComplexTypeFieldDescriptor Descriptor_;
};

TSkiffToYsonConverter CreateSimpleSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema)
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
#undef CASE
        default:
            Y_UNREACHABLE();
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
        YCHECK(skiffNesting >= 0);
        YCHECK(ysonNesting > 0);

        YCHECK(skiffNesting <= ysonNesting);
        YCHECK(ysonNesting <= skiffNesting + 1);
    }

    void operator () (TCheckedInDebugSkiffParser* parser, IYsonConsumer* consumer)
    {
        for (int i = 0; i < OuterFill_; ++i) {
            consumer->OnBeginList();
        }

        int outerOptionalsFilled = 0;
        for (; outerOptionalsFilled < OuterTranslate_; ++outerOptionalsFilled) {
            auto tag = parser->ParseVariant8Tag();
            if (tag == 0) {
                consumer->OnEntity();
                goto write_list_ends;
            } else if (tag == 1) {
                consumer->OnBeginList();
            } else {
                ThrowUnexpectedVariant8Tag(tag);
            }
        }

        if (InnerTranslate_) {
            auto tag = parser->ParseVariant8Tag();
            if (tag == 0) {
                consumer->OnEntity();
                goto write_list_ends;
            } else if (tag != 1) {
                ThrowUnexpectedVariant8Tag(tag);
            }
        }
        InnerConverter_(parser, consumer);

write_list_ends:
        const int toClose = outerOptionalsFilled + OuterFill_;
        for (int i = 0; i < toClose; ++i) {
            consumer->OnEndList();
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

TSkiffToYsonConverter CreateOptionalSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema)
{
    auto match = MatchOptionalTypes(descriptor, skiffSchema);
    auto innerConverter = CreateSkiffToYsonConverter(std::move(match.InnerTypes.first), match.InnerTypes.second);
    return TOptionalSkiffToYsonConverterImpl(innerConverter, std::move(descriptor), match.LogicalNesting, match.SkiffNesting);
}

TSkiffToYsonConverter CreateListSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema)
{
    auto match = MatchListTypes(descriptor, skiffSchema);
    auto innerConverter = CreateSkiffToYsonConverter(std::move(match.first), match.second);

    return [innerConverter = innerConverter, descriptor=std::move(descriptor)](TCheckedInDebugSkiffParser* parser, IYsonConsumer* consumer) {
        consumer->OnBeginList();
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
            consumer->OnListItem();
            innerConverter(parser, consumer);
        }
        consumer->OnEndList();
    };
}

TSkiffToYsonConverter CreateStructSkiffToYsonConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema)
{
    const auto insertEntity = [](TCheckedInDebugSkiffParser* /*parser*/, IYsonConsumer* consumer) {
        consumer->OnEntity();
    };

    auto structMatch = MatchStructTypes(descriptor, skiffSchema);
    std::vector<TSkiffToYsonConverter> converterList;
    for (const auto& [fieldDescriptor, fieldSkiffSchema] : structMatch) {
        if (fieldSkiffSchema) {
            converterList.emplace_back(CreateSkiffToYsonConverter(fieldDescriptor, fieldSkiffSchema));
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

    return [converterList = converterList](TCheckedInDebugSkiffParser* parser, IYsonConsumer* consumer) {
        consumer->OnBeginList();
        for (const auto& converter : converterList) {
            converter(parser, consumer);
        }
        consumer->OnEndList();
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYsonToSkiffConverter CreateYsonToSkiffConverter(
    TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    const TYsonToSkiffConverterConfig& config)
{
    TYsonToSkiffCreatorContext context;
    // CreateYsonToSkiffConverterImpl will increment NestingLevel to 0 for the top level element.
    context.NestingLevel = -1;
    return CreateYsonToSkiffConverterImpl(std::move(descriptor), skiffSchema, context, config);
}

TSkiffToYsonConverter CreateSkiffToYsonConverter(TComplexTypeFieldDescriptor descriptor, const TSkiffSchemaPtr& skiffSchema)
{
    const auto& logicalType = descriptor.GetType();
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CreateSimpleSkiffToYsonConverter(std::move(descriptor), skiffSchema);
        case ELogicalMetatype::Optional:
            return CreateOptionalSkiffToYsonConverter(std::move(descriptor), skiffSchema);
        case ELogicalMetatype::List:
            return CreateListSkiffToYsonConverter(std::move(descriptor), skiffSchema);
        case ELogicalMetatype::Struct:
            return CreateStructSkiffToYsonConverter(std::move(descriptor), skiffSchema);
    }
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
