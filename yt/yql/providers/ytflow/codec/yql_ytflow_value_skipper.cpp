#include "yql_ytflow_value_skipper.h"

#include <yql/essentials/utils/yql_panic.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/core/yson/pull_parser.h>

#include <util/string/cast.h>


namespace NYql::NYtflow::NCodec::NPrivate {

struct TValueSkipper: public IValueSkipper {
public:
    void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TLogicalType* ytType
    ) const override {
        switch (ytType->GetMetatype()) {
        case NYT::NTableClient::ELogicalMetatype::Simple: {
            auto* ytDataType = static_cast<
                const NYT::NTableClient::TSimpleLogicalType*>(ytType);

            SkipValue(ysonParser, ytDataType);

            break;
        }

        case NYT::NTableClient::ELogicalMetatype::Decimal: {
            auto* ytDecimalType = static_cast<
                const NYT::NTableClient::TDecimalLogicalType*>(ytType);

            SkipValue(ysonParser, ytDecimalType);

            break;
        }

        case NYT::NTableClient::ELogicalMetatype::Tuple: {
            auto* ytTupleType = static_cast<
                const NYT::NTableClient::TTupleLogicalType*>(ytType);

            SkipValue(ysonParser, ytTupleType);

            break;
        }

        case NYT::NTableClient::ELogicalMetatype::Struct: {
            auto* ytStructType = static_cast<
                const NYT::NTableClient::TStructLogicalType*>(ytType);

            SkipValue(ysonParser, ytStructType);

            break;
        }

        case NYT::NTableClient::ELogicalMetatype::List: {
            auto* ytListType = static_cast<
                const NYT::NTableClient::TListLogicalType*>(ytType);

            SkipValue(ysonParser, ytListType);

            break;
        }

        case NYT::NTableClient::ELogicalMetatype::Optional: {
            auto* ytOptionalType = static_cast<
                const NYT::NTableClient::TOptionalLogicalType*>(ytType);

            SkipValue(ysonParser, ytOptionalType);

            break;
        }

        case NYT::NTableClient::ELogicalMetatype::Dict: {
            auto* ytDictType = static_cast<
                const NYT::NTableClient::TDictLogicalType*>(ytType);

            SkipValue(ysonParser, ytDictType);

            break;
        }

        case NYT::NTableClient::ELogicalMetatype::Tagged: {
            auto* ytTaggedType = static_cast<
                const NYT::NTableClient::TTaggedLogicalType*>(ytType);

            SkipValue(ysonParser, ytTaggedType);

            break;
        }

        case NYT::NTableClient::ELogicalMetatype::VariantTuple: {
            auto* ytVariantTupleType = static_cast<
                const NYT::NTableClient::TVariantTupleLogicalType*>(ytType);

            SkipValue(ysonParser, ytVariantTupleType);

            break;
        }

        case NYT::NTableClient::ELogicalMetatype::VariantStruct: {
            auto* ytVariantStructType = static_cast<
                const NYT::NTableClient::TVariantStructLogicalType*>(ytType);

            SkipValue(ysonParser, ytVariantStructType);

            break;
        }

        default:
            YQL_ENSURE(
                false, "Unsupported metatype: " << ToString(ytType->GetMetatype()));
        }
    }

    void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TSimpleLogicalType* ytType
    ) const {
        switch (ytType->GetElement()) {
        case NYT::NTableClient::ESimpleLogicalValueType::String:
        case NYT::NTableClient::ESimpleLogicalValueType::Json:
        case NYT::NTableClient::ESimpleLogicalValueType::Utf8:
        case NYT::NTableClient::ESimpleLogicalValueType::Any:
        case NYT::NTableClient::ESimpleLogicalValueType::Uuid:
            ysonParser.ParseString();
            break;

        case NYT::NTableClient::ESimpleLogicalValueType::Int8:
        case NYT::NTableClient::ESimpleLogicalValueType::Int16:
        case NYT::NTableClient::ESimpleLogicalValueType::Int32:
        case NYT::NTableClient::ESimpleLogicalValueType::Int64:
        case NYT::NTableClient::ESimpleLogicalValueType::Interval:
        case NYT::NTableClient::ESimpleLogicalValueType::Date32:
        case NYT::NTableClient::ESimpleLogicalValueType::Datetime64:
        case NYT::NTableClient::ESimpleLogicalValueType::Timestamp64:
        case NYT::NTableClient::ESimpleLogicalValueType::Interval64:
            ysonParser.ParseInt64();
            break;

        case NYT::NTableClient::ESimpleLogicalValueType::Uint8:
        case NYT::NTableClient::ESimpleLogicalValueType::Uint16:
        case NYT::NTableClient::ESimpleLogicalValueType::Uint32:
        case NYT::NTableClient::ESimpleLogicalValueType::Uint64:
        case NYT::NTableClient::ESimpleLogicalValueType::Date:
        case NYT::NTableClient::ESimpleLogicalValueType::Datetime:
        case NYT::NTableClient::ESimpleLogicalValueType::Timestamp:
            ysonParser.ParseUint64();
            break;

        case NYT::NTableClient::ESimpleLogicalValueType::Float:
        case NYT::NTableClient::ESimpleLogicalValueType::Double:
            ysonParser.ParseDouble();
            break;

        case NYT::NTableClient::ESimpleLogicalValueType::Boolean:
            ysonParser.ParseBoolean();
            break;

        default:
            YQL_ENSURE(
                false,
                "Unsupported simple logical type: " << ToString(ytType->GetElement()));
        }
    }

    void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TDecimalLogicalType* /*ytType*/
    ) const {
        ysonParser.ParseString();
    }

    void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TTupleLogicalType* ytType
    ) const {
        ysonParser.ParseBeginList();

        for (const auto& ytItemType: ytType->GetElements()) {
            SkipValue(ysonParser, ytItemType.Get());
        }

        ysonParser.ParseEndList();
    }

    void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TStructLogicalType* ytType
    ) const {
        ysonParser.ParseBeginList();

        for (const auto& ytField: ytType->GetFields()) {
            SkipValue(ysonParser, ytField.Type.Get());
        }

        ysonParser.ParseEndList();
    }

    void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TListLogicalType* ytType
    ) const {
        auto* ytItemType = ytType->GetElement().Get();

        ysonParser.ParseBeginList();

        while (!ysonParser.IsEndList()) {
            SkipValue(ysonParser, ytItemType);
        }

        ysonParser.ParseEndList();
    }

    void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TOptionalLogicalType* ytType
    ) const {
        if (ysonParser.IsEntity()) {
            ysonParser.Next();
            return;
        }

        auto* ytItemType = ytType->GetElement().Get();
        SkipValue(ysonParser, ytItemType);
    }

    void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TDictLogicalType* ytType
    ) const {
        auto* ytKeyType = ytType->GetKey().Get();
        auto* ytPayloadType = ytType->GetValue().Get();

        ysonParser.ParseBeginList();

        while (!ysonParser.IsEndList()) {
            ysonParser.ParseBeginList();

            SkipValue(ysonParser, ytKeyType);
            SkipValue(ysonParser, ytPayloadType);

            ysonParser.ParseEndList();
        }

        ysonParser.ParseEndList();
    }

    void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TTaggedLogicalType* ytType
    ) const {
        auto* ytBaseType = ytType->GetElement().Get();
        SkipValue(ysonParser, ytBaseType);
    }

    void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TVariantTupleLogicalType* ytType
    ) const {
        ysonParser.ParseBeginList();

        auto ytAlternativeIndex = static_cast<ui32>(ysonParser.ParseInt64());
        auto* ytAlternativeType = ytType->GetElements()[ytAlternativeIndex].Get();

        SkipValue(ysonParser, ytAlternativeType);

        ysonParser.ParseEndList();
    }

    void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TVariantStructLogicalType* ytType
    ) const {
        ysonParser.ParseBeginList();

        auto ytAlternativeIndex = static_cast<ui32>(ysonParser.ParseInt64());
        auto* ytAlternativeType = ytType->GetFields()[ytAlternativeIndex].Type.Get();

        SkipValue(ysonParser, ytAlternativeType);

        ysonParser.ParseEndList();
    }
};

THolder<IValueSkipper> CreateValueSkipper() {
    return MakeHolder<TValueSkipper>();
}

} // namespace NYql::NYtflow::NCodec::NPrivate
