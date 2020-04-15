#include "skiff_parser.h"
#include "skiff_yson_converter.h"

#include "helpers.h"
#include "parser.h"
#include "yson_map_to_unversioned_value.h"

#include <yt/library/skiff/schema_match.h>
#include <yt/library/skiff/parser.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/table_consumer.h>
#include <yt/client/table_client/value_consumer.h>

#include <yt/core/concurrency/coroutine.h>

#include <yt/core/yson/parser.h>
#include <yt/core/yson/token_writer.h>

#include <util/generic/strbuf.h>
#include <util/stream/zerocopy.h>
#include <util/stream/buffer.h>

namespace NYT::NFormats {

using namespace NTableClient;
using namespace NSkiff;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using TSkiffToUnversionedValueConverter = std::function<void(TCheckedInDebugSkiffParser*, IValueConsumer*)>;

template<EWireType wireType, bool isOptional>
class TSimpleTypeConverterImpl
{
public:
    explicit TSimpleTypeConverterImpl(ui16 columnId, TYsonToUnversionedValueConverter* ysonConverter = nullptr)
        : ColumnId_(columnId)
        , YsonConverter_(ysonConverter)
    {}

    void operator()(TCheckedInDebugSkiffParser* parser, IValueConsumer* valueConsumer)
    {
        if constexpr (isOptional) {
            ui8 tag = parser->ParseVariant8Tag();
            if (tag == 0) {
                valueConsumer->OnValue(MakeUnversionedNullValue(ColumnId_));
                return;
            } else if (tag > 1) {
                const auto name = valueConsumer->GetNameTable()->GetName(ColumnId_);
                THROW_ERROR_EXCEPTION(
                    "Found bad variant8 tag %Qv when parsing optional field %Qv",
                    tag,
                    name);
            }
        }
        if constexpr (wireType == EWireType::Yson32) {
            YT_VERIFY(YsonConverter_);
            auto ysonString = parser->ParseYson32();
            YsonConverter_->SetColumnIndex(ColumnId_);
            {
                auto consumer = YsonConverter_->SwitchToTable(0);
                YT_VERIFY(consumer == valueConsumer);
            }
            ParseYsonStringBuffer(ysonString, NYson::EYsonType::Node, YsonConverter_);
        } else if constexpr (wireType == EWireType::Int64) {
            valueConsumer->OnValue(MakeUnversionedInt64Value(parser->ParseInt64(), ColumnId_));
        } else if constexpr (wireType == EWireType::Uint64) {
            valueConsumer->OnValue(MakeUnversionedUint64Value(parser->ParseUint64(), ColumnId_));
        } else if constexpr (wireType == EWireType::Double) {
            valueConsumer->OnValue(MakeUnversionedDoubleValue(parser->ParseDouble(), ColumnId_));
        } else if constexpr (wireType == EWireType::Boolean) {
            valueConsumer->OnValue(MakeUnversionedBooleanValue(parser->ParseBoolean(), ColumnId_));
        } else if constexpr (wireType == EWireType::String32) {
            valueConsumer->OnValue(MakeUnversionedStringValue(parser->ParseString32(), ColumnId_));
        } else if constexpr (wireType == EWireType::Nothing) {
            valueConsumer->OnValue(MakeUnversionedNullValue(ColumnId_));
        } else {
            static_assert(wireType == EWireType::Int64);
        }
    }

private:
    const ui16 ColumnId_;
    TYsonToUnversionedValueConverter* YsonConverter_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

TSkiffToUnversionedValueConverter CreateSimpleValueConverter(
    EWireType wireType,
    bool required,
    ui16 columnId,
    TYsonToUnversionedValueConverter* ysonConverter)
{
    switch (wireType) {
#define CASE(x) \
        case x: \
            do { \
                if (required) { \
                    return TSimpleTypeConverterImpl<x, false>(columnId, ysonConverter); \
                } else { \
                    return TSimpleTypeConverterImpl<x, true>(columnId, ysonConverter); \
                } \
            } while (0)

        CASE(EWireType::Int64);
        CASE(EWireType::Uint64);
        CASE(EWireType::Boolean);
        CASE(EWireType::Double);
        CASE(EWireType::String32);
        CASE(EWireType::Yson32);
#undef CASE
        case EWireType::Nothing:
            YT_VERIFY(required);
            return TSimpleTypeConverterImpl<EWireType::Nothing, false>(columnId, ysonConverter);

        default:
            YT_ABORT();
    }
}

class TComplexValueConverter
{
public:
    TComplexValueConverter(TSkiffToYsonConverter converter, ui16 columnId)
        : Converter_(std::move(converter))
        , ColumnId_(columnId)
    { }

    void operator() (TCheckedInDebugSkiffParser* parser, IValueConsumer* valueConsumer)
    {
        Buffer_.Clear();
        {
            TBufferOutput out(Buffer_);
            NYson::TCheckedInDebugYsonTokenWriter ysonTokenWriter(&out);
            Converter_(parser, &ysonTokenWriter);
            ysonTokenWriter.Finish();
        }
        auto value = TStringBuf(Buffer_.Data(), Buffer_.Size());
        const auto entity = AsStringBuf("#");
        if (value == entity) {
            valueConsumer->OnValue(MakeUnversionedNullValue(ColumnId_));
        } else {
            valueConsumer->OnValue(MakeUnversionedCompositeValue(value, ColumnId_));
        }
    }

private:
    const TSkiffToYsonConverter Converter_;
    const ui16 ColumnId_;
    TBuffer Buffer_;
};

TSkiffToUnversionedValueConverter CreateComplexValueConverter(
    NTableClient::TComplexTypeFieldDescriptor descriptor,
    const TSkiffSchemaPtr& skiffSchema,
    ui16 columnId,
    bool sparseColumn)
{
    TSkiffToYsonConverterConfig config;
    config.AllowOmitTopLevelOptional = sparseColumn;
    auto converter = CreateSkiffToYsonConverter(std::move(descriptor), skiffSchema, config);
    return TComplexValueConverter(converter, columnId);
}

////////////////////////////////////////////////////////////////////////////////

class TSkiffParserImpl
{
public:
    TSkiffParserImpl(const TSkiffSchemaPtr& skiffSchema, const TTableSchema& tableSchema, IValueConsumer* valueConsumer)
        : SkiffSchemaList_({skiffSchema})
        , ValueConsumer_(valueConsumer)
        , YsonToUnversionedValueConverter_(EComplexTypeMode::Named, ValueConsumer_)
        , OtherColumnsConsumer_(EComplexTypeMode::Named, ValueConsumer_)
    {
        THashMap<TString, const TColumnSchema*> columnSchemas;
        for (const auto& column : tableSchema.Columns()) {
            columnSchemas[column.Name()] = &column;
        }

        auto genericTableDescriptions = CreateTableDescriptionList(
            SkiffSchemaList_, RangeIndexColumnName, RowIndexColumnName);

        for (int tableIndex = 0; tableIndex < genericTableDescriptions.size(); ++tableIndex) {
            const auto& genericTableDescription = genericTableDescriptions[tableIndex];
            auto& parserTableDescription = TableDescriptions_.emplace_back();
            parserTableDescription.HasOtherColumns = genericTableDescription.HasOtherColumns;
            for (const auto& fieldDescription : genericTableDescription.DenseFieldDescriptionList) {
                const auto columnId = ValueConsumer_->GetNameTable()->GetIdOrRegisterName(fieldDescription.Name());
                TSkiffToUnversionedValueConverter converter;
                auto columnSchema = columnSchemas.FindPtr(fieldDescription.Name());
                try {
                    if (columnSchema && !(*columnSchema)->SimplifiedLogicalType()) {
                        converter = CreateComplexValueConverter(
                            TComplexTypeFieldDescriptor(fieldDescription.Name(), (*columnSchema)->LogicalType()),
                            fieldDescription.Schema(),
                            columnId,
                            /*sparseColumn*/ false);
                    } else {
                        converter = CreateSimpleValueConverter(
                            fieldDescription.ValidatedSimplify(),
                            fieldDescription.IsRequired(),
                            columnId,
                            &YsonToUnversionedValueConverter_);
                    }
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Cannot create skiff parser for table #%v",
                        tableIndex)
                        << ex;
                }
                parserTableDescription.DenseFieldConverters.emplace_back(converter);
            }

            for (const auto& fieldDescription : genericTableDescription.SparseFieldDescriptionList) {
                const auto columnId = ValueConsumer_->GetNameTable()->GetIdOrRegisterName(fieldDescription.Name());
                TSkiffToUnversionedValueConverter converter;
                auto columnSchema = columnSchemas.FindPtr(fieldDescription.Name());
                try {
                    if (columnSchema && !(*columnSchema)->SimplifiedLogicalType()) {
                        converter = CreateComplexValueConverter(
                            TComplexTypeFieldDescriptor(fieldDescription.Name(), (*columnSchema)->LogicalType()),
                            fieldDescription.Schema(),
                            columnId,
                            /*sparseColumn*/ true);
                    } else {
                        converter = CreateSimpleValueConverter(
                            fieldDescription.ValidatedSimplify(),
                            fieldDescription.IsRequired(),
                            columnId,
                            &YsonToUnversionedValueConverter_);
                    }
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Cannot create skiff parser for table #%v",
                        tableIndex)
                        << ex;
                }
                parserTableDescription.SparseFieldConverters.emplace_back(converter);
            }
        }
    }

    void DoParse(IZeroCopyInput* stream)
    {
        Parser_ = std::make_unique<TCheckedInDebugSkiffParser>(CreateVariant16Schema(SkiffSchemaList_), stream);

        while (Parser_->HasMoreData()) {
            auto tag = Parser_->ParseVariant16Tag();
            if (tag > 0) {
                THROW_ERROR_EXCEPTION("Unkwnown table index varint16 tag %v",
                    tag);
            }
            ValueConsumer_->OnBeginRow();

            for (const auto& converter : TableDescriptions_[tag].DenseFieldConverters) {
                converter(Parser_.get(), ValueConsumer_);
            }

            if (!TableDescriptions_[tag].SparseFieldConverters.empty()) {
                for (auto sparseFieldIdx = Parser_->ParseVariant16Tag();
                     sparseFieldIdx != EndOfSequenceTag<ui16>();
                     sparseFieldIdx = Parser_->ParseVariant16Tag()) {
                    if (sparseFieldIdx >= TableDescriptions_[tag].SparseFieldConverters.size()) {
                        THROW_ERROR_EXCEPTION("Bad sparse field index %Qv, total sparse field count %Qv",
                            sparseFieldIdx,
                            TableDescriptions_[tag].SparseFieldConverters.size());
                    }

                    const auto& converter = TableDescriptions_[tag].SparseFieldConverters[sparseFieldIdx];
                    converter(Parser_.get(), ValueConsumer_);
                }
            }

            if (TableDescriptions_[tag].HasOtherColumns) {
                auto buf = Parser_->ParseYson32();
                ParseYsonStringBuffer(
                    buf,
                    NYson::EYsonType::Node,
                    &OtherColumnsConsumer_);
            }

            ValueConsumer_->OnEndRow();
        }
    }

    ui64 GetReadBytesCount()
    {
        return Parser_->GetReadBytesCount();
    }

private:
    struct TTableDescription
    {
        std::vector<TSkiffToUnversionedValueConverter> DenseFieldConverters;
        std::vector<TSkiffToUnversionedValueConverter> SparseFieldConverters;
        bool HasOtherColumns = false;
    };

    const TSkiffSchemaList SkiffSchemaList_;

    IValueConsumer* const ValueConsumer_;

    TYsonToUnversionedValueConverter YsonToUnversionedValueConverter_;
    TYsonMapToUnversionedValueConverter OtherColumnsConsumer_;

    std::unique_ptr<TCheckedInDebugSkiffParser> Parser_;
    // TODO(ermolovd): this actually must
    std::vector<TTableDescription> TableDescriptions_;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffPushParser
    : public IParser
{
public:
    TSkiffPushParser(const TSkiffSchemaPtr& skiffSchema, const TTableSchema& tableSchema, IValueConsumer* consumer)
        : ParserImpl_(std::make_unique<TSkiffParserImpl>(skiffSchema, tableSchema, consumer))
        , ParserCoroPipe_(
            BIND(
                [=](IZeroCopyInput* stream) {
                    ParserImpl_->DoParse(stream);
                }))
    {}

    void Read(TStringBuf data) override
    {
        if (!data.empty()) {
            ParserCoroPipe_.Feed(data);
        }
    }

    void Finish() override
    {
        ParserCoroPipe_.Finish();
    }

private:
    std::unique_ptr<TSkiffParserImpl> ParserImpl_;
    TCoroPipe ParserCoroPipe_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace // anonymous

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForSkiff(
    TSkiffSchemaPtr skiffSchema,
    const TTableSchema& tableSchema,
    IValueConsumer* consumer)
{
    auto tableDescriptionList = CreateTableDescriptionList({skiffSchema}, RangeIndexColumnName, RowIndexColumnName);
    if (tableDescriptionList.size() != 1) {
        THROW_ERROR_EXCEPTION("Expected to have single table, actual table description count %Qv",
            tableDescriptionList.size());
    }
    return std::make_unique<TSkiffPushParser>(
        skiffSchema,
        tableSchema,
        consumer);
}

std::unique_ptr<IParser> CreateParserForSkiff(
    IValueConsumer* consumer,
    TSkiffFormatConfigPtr config,
    int tableIndex)
{
    auto skiffSchemas = ParseSkiffSchemas(config->SkiffSchemaRegistry, config->TableSkiffSchemas);
    if (tableIndex >= static_cast<int>(skiffSchemas.size())) {
        THROW_ERROR_EXCEPTION("Skiff format config does not describe table #%v",
            tableIndex);
    }
    if (tableIndex == 0 && config->OverrideIntermediateTableSchema) {
        if (!IsTrivialIntermediateSchema(consumer->GetSchema())) {
            THROW_ERROR_EXCEPTION("Cannot use \"override_intermediate_table_schema\" since output table #0 has nontrivial schema")
                << TErrorAttribute("schema", consumer->GetSchema());
        }
        return CreateParserForSkiff(
            skiffSchemas[tableIndex],
            *config->OverrideIntermediateTableSchema,
            consumer);
    } else {
        return CreateParserForSkiff(
            skiffSchemas[tableIndex],
            consumer);
    }
}

std::unique_ptr<IParser> CreateParserForSkiff(
    TSkiffSchemaPtr skiffSchema,
    IValueConsumer* consumer)
{
    return CreateParserForSkiff(skiffSchema, consumer->GetSchema(), consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
