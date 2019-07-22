#include "web_json_writer.h"

#include "config.h"
#include "format.h"
#include "helpers.h"
#include "public.h"
#include "schemaless_writer_adapter.h"
#include "yql_yson_converter.h"

#include <yt/client/table_client/logical_type.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/yson/format.h>
#include <yt/core/yson/pull_parser.h>

#include <yt/core/json/json_writer.h>
#include <yt/core/json/config.h>

#include <yt/core/ytree/fluent.h>

#include <util/generic/buffer.h>

namespace NYT::NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NJson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto ContextBufferCapacity = 1_MB;

////////////////////////////////////////////////////////////////////////////////

class TWrittenSizeAccountedOutputStream
    : public IOutputStream
{
public:
    explicit TWrittenSizeAccountedOutputStream(
        std::unique_ptr<IOutputStream> underlyingStream = nullptr)
    {
        Reset(std::move(underlyingStream));
    }

    void Reset(std::unique_ptr<IOutputStream> underlyingStream)
    {
        UnderlyingStream_ = std::move(underlyingStream);
        WrittenSize_ = 0;
    }

    i64 GetWrittenSize() const
    {
        return WrittenSize_;
    }

protected:
    // For simplicity we do not override DoWriteV and DoWriteC methods here.
    // Overriding DoWrite method is enough for local usage.
    virtual void DoWrite(const void* buf, size_t length) override
    {
        if (UnderlyingStream_) {
            UnderlyingStream_->Write(buf, length);
            WrittenSize_ += length;
        }
    }

    virtual void DoFlush() override
    {
        if (UnderlyingStream_) {
            UnderlyingStream_->Flush();
        }
    }

    virtual void DoFinish() override
    {
        if (UnderlyingStream_) {
            UnderlyingStream_->Finish();
        }
    }

private:
    std::unique_ptr<IOutputStream> UnderlyingStream_;

    i64 WrittenSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TWebJsonColumnFilter
{
public:
    TWebJsonColumnFilter(int maxSelectedColumnCount, std::optional<THashSet<TString>> names)
        : MaxSelectedColumnCount_(maxSelectedColumnCount)
        , Names_(std::move(names))
    { }

    bool Accept(ui16 columnId, TStringBuf columnName)
    {
        if (Names_) {
            return AcceptByNames(columnId, columnName);
        }
        return AcceptByMaxCount(columnId, columnName);
    }

private:
    const int MaxSelectedColumnCount_;
    std::optional<THashSet<TString>> Names_;

    THashSet<ui16> AcceptedColumnIds_;

    bool AcceptByNames(ui16 columnId, TStringBuf columnName)
    {
        return Names_->contains(columnName);
    }

    bool AcceptByMaxCount(ui16 columnId, TStringBuf columnName)
    {
        if (AcceptedColumnIds_.size() < MaxSelectedColumnCount_) {
            AcceptedColumnIds_.insert(columnId);
            return true;
        }
        return AcceptedColumnIds_.contains(columnId);
    }
};

////////////////////////////////////////////////////////////////////////////////

TWebJsonColumnFilter CreateWebJsonColumnFilter(const TWebJsonFormatConfigPtr& webJsonConfig)
{
    std::optional<THashSet<TString>> columnNames;
    if (webJsonConfig->ColumnNames) {
        columnNames.emplace();
        for (const auto& columnName : *webJsonConfig->ColumnNames) {
            if (!columnNames->insert(columnName).second) {
                THROW_ERROR_EXCEPTION("Duplicate column name %Qv in \"column_names\" parameter of web_json format config",
                    columnName);
            }
        }
    }
    return TWebJsonColumnFilter(webJsonConfig->MaxSelectedColumnCount, std::move(columnNames));
}

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetSimpleYqlTypeName(ESimpleLogicalValueType type)
{
    switch (type) {
        case ESimpleLogicalValueType::Double:
            return AsStringBuf("Double");
        case ESimpleLogicalValueType::Boolean:
            return AsStringBuf("Boolean");
        case ESimpleLogicalValueType::String:
            return AsStringBuf("String");
        case ESimpleLogicalValueType::Utf8:
            return AsStringBuf("Utf8");
        case ESimpleLogicalValueType::Any:
            return AsStringBuf("Yson");
        case ESimpleLogicalValueType::Int8:
            return AsStringBuf("Int8");
        case ESimpleLogicalValueType::Int16:
            return AsStringBuf("Int16");
        case ESimpleLogicalValueType::Int32:
            return AsStringBuf("Int32");
        case ESimpleLogicalValueType::Int64:
            return AsStringBuf("Int64");
        case ESimpleLogicalValueType::Uint8:
            return AsStringBuf("Uint8");
        case ESimpleLogicalValueType::Uint16:
            return AsStringBuf("Uint16");
        case ESimpleLogicalValueType::Uint32:
            return AsStringBuf("Uint32");
        case ESimpleLogicalValueType::Uint64:
            return AsStringBuf("Uint64");
        case ESimpleLogicalValueType::Date:
            return AsStringBuf("Date");
        case ESimpleLogicalValueType::Datetime:
            return AsStringBuf("Datetime");
        case ESimpleLogicalValueType::Timestamp:
            return AsStringBuf("Timestamp");
        case ESimpleLogicalValueType::Interval:
            return AsStringBuf("Interval");
        case ESimpleLogicalValueType::Null:
            // This case must have been processed earlier.
            YT_ABORT();
    }
    YT_ABORT();
}

void SerializeAsYqlType(TFluentAny fluent, const TLogicalTypePtr& type)
{
     auto serializeStruct = [] (TFluentList fluentList, const TStructLogicalTypeBase& structType) {
        fluentList
            .Item().Value("StructType")
            .Item().DoListFor(structType.GetFields(), [] (TFluentList innerFluentList, const TStructField& field) {
                innerFluentList
                    .Item()
                    .BeginList()
                        .Item().Value(field.Name)
                        .Item().Do([&] (TFluentAny innerFluent) {
                            SerializeAsYqlType(innerFluent, field.Type);
                        })
                    .EndList();
            });
    };

    auto serializeTuple = [] (TFluentList fluentList, const TTupleLogicalTypeBase& tupleType) {
        fluentList
            .Item().Value("TupleType")
            .Item().DoListFor(tupleType.GetElements(), [&] (TFluentList innerFluentList, const TLogicalTypePtr& element) {
                innerFluentList
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, element);
                    });
            });
    };

    auto build = [&] (TFluentList fluentList){
        switch (type->GetMetatype()) {
            case ELogicalMetatype::Simple:
                if (type->AsSimpleTypeRef().GetElement() == ESimpleLogicalValueType::Null) {
                    fluentList
                        .Item().Value("NullType");
                } else {
                    fluentList
                        .Item().Value("DataType")
                        .Item().Value(GetSimpleYqlTypeName(type->AsSimpleTypeRef().GetElement()));
                }
                return;
            case ELogicalMetatype::Optional:
                fluentList
                    .Item().Value("OptionalType")
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type->AsOptionalTypeRef().GetElement());
                    });
                return;
            case ELogicalMetatype::List:
                fluentList
                    .Item().Value("ListType")
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type->AsListTypeRef().GetElement());
                    });
                return;
            case ELogicalMetatype::Struct:
                serializeStruct(fluentList, type->AsStructTypeRef());
                return;
            case ELogicalMetatype::Tuple:
                serializeTuple(fluentList, type->AsTupleTypeRef());
                return;
            case ELogicalMetatype::VariantStruct:
                fluentList
                    .Item().Value("VariantType")
                    .Item().DoList([&] (TFluentList fluentList) {
                        serializeStruct(fluentList, type->AsVariantStructTypeRef());
                    });
                return;
            case ELogicalMetatype::VariantTuple:
                fluentList
                    .Item().Value("VariantType")
                    .Item().DoList([&] (TFluentList fluentList) {
                        serializeTuple(fluentList, type->AsVariantTupleTypeRef());
                    });
                return;
            case ELogicalMetatype::Dict:
                fluentList
                    .Item().Value("DictType")
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type->AsDictTypeRef().GetKey());
                    })
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type->AsDictTypeRef().GetValue());
                    });
                return;
            case ELogicalMetatype::Tagged:
                fluentList
                    .Item().Value("TaggedType")
                    .Item().Value(type->AsTaggedTypeRef().GetTag())
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type->AsTaggedTypeRef().GetElement());
                    });
                return;
        }
        YT_ABORT();
    };

    fluent.DoList(build);
}

////////////////////////////////////////////////////////////////////////////////

class TYqlValueWriter
{
public:
    TYqlValueWriter(
        const TWebJsonFormatConfigPtr& config,
        const std::vector<TTableSchema>& schemas,
        IJsonConsumer* consumer)
        : Consumer_(consumer)
        , TableIndexToColumnIdToTypeIndex_(schemas.size())
    {
        YT_VERIFY(config->ValueFormat == EWebJsonValueFormat::Yql);

        for (auto valueType : TEnumTraits<EValueType>::GetDomainValues()) {
            if (IsValueType(valueType) || valueType == EValueType::Null) {
                Types_.push_back(SimpleLogicalType(GetLogicalType(valueType)));
                Converters_.push_back(CreateUnversionedValueToYqlConverter(Types_.back()));
                ValueTypeToTypeIndex_[valueType] = static_cast<int>(Types_.size()) - 1;
            } else {
                ValueTypeToTypeIndex_[valueType] = UnknownTypeIndex;
            }
        }

        for (int tableIndex = 0; tableIndex != static_cast<int>(schemas.size()); ++tableIndex) {
            const auto& schema = schemas[tableIndex];
            for (const auto& column : schema.Columns()) {
                Types_.push_back(column.LogicalType());
                Converters_.push_back(CreateUnversionedValueToYqlConverter(column.LogicalType()));
                auto [it, inserted] = TableIndexAndColumnNameToTypeIndex_.emplace(
                    std::make_pair(tableIndex, column.Name()),
                    static_cast<int>(Types_.size()) - 1);
                YT_VERIFY(inserted);
            }
        }
    }

    void RegisterColumnNames(const TNameTablePtr& nameTable)
    {
        TableIndexId_ = nameTable->GetIdOrRegisterName(TableIndexColumnName);
    }

    void ProcessRow(TUnversionedRow row)
    {
        YT_VERIFY(TableIndexId_ != -1);
        // Search for table index switch. If it is not found we assume the table index has not changed.
        for (auto value : row) {
            if (value.Id == TableIndexId_) {
                CurrentTableIndex_ = value.Data.Int64;
                break;
            }
        }
    }

    void WriteValue(TStringBuf columnName, TUnversionedValue value)
    {
        auto typeIndex = GetTypeIndex(CurrentTableIndex_, value.Id, columnName, value.Type);
        BuildYsonFluently(&Consumer_)
            .BeginList()
                .Item().Do([&, value] (TFluentAny fluent) {
                    Converters_[typeIndex](value, &Consumer_);
                })
                .Item().Value(::ToString(typeIndex))
            .EndList();
    }

    void WriteMetaInfo()
    {
        Consumer_.OnKeyedItem("yql_type_registry");
        BuildYsonFluently(&Consumer_)
            .DoListFor(Types_, [&] (TFluentList fluentList, const TLogicalTypePtr& type) {
                fluentList
                    .Item().Do([&] (TFluentAny innerFluent) {
                        SerializeAsYqlType(innerFluent, type);
                    });
            });
    }

    static TJsonFormatConfigPtr GetJsonConfig(const TWebJsonFormatConfigPtr& /* webJsonConfig */)
    {
        auto config = New<TJsonFormatConfig>();
        config->EncodeUtf8 = false;
        return config;
    }

private:
    static constexpr int UnknownTypeIndex = -1;
    static constexpr int UnschematizedTypeIndex = -2;

    TYqlJsonConsumer Consumer_;
    std::vector<TUnversionedValueToYqlConverter> Converters_;
    std::vector<TLogicalTypePtr> Types_;
    std::vector<std::vector<int>> TableIndexToColumnIdToTypeIndex_;
    THashMap<std::pair<int, TString>, int> TableIndexAndColumnNameToTypeIndex_;
    TEnumIndexedVector<EValueType, int> ValueTypeToTypeIndex_;
    TBuffer BufferForWriters_;

    int CurrentTableIndex_ = 0;
    int TableIndexId_ = -1;

private:
    int GetTypeIndex(int tableIndex, ui16 columnId, TStringBuf columnName, EValueType valueType)
    {
        YT_VERIFY(0 <= tableIndex && tableIndex < static_cast<int>(TableIndexToColumnIdToTypeIndex_.size()));
        auto& columnIdToTypeIndex = TableIndexToColumnIdToTypeIndex_[tableIndex];
        if (columnId >= columnIdToTypeIndex.size()) {
            columnIdToTypeIndex.resize(columnId + 1, UnknownTypeIndex);
        }

        auto typeIndex = columnIdToTypeIndex[columnId];
        if (typeIndex == UnschematizedTypeIndex) {
            typeIndex = ValueTypeToTypeIndex_[valueType];
        } else if (typeIndex == UnknownTypeIndex) {
            auto it = TableIndexAndColumnNameToTypeIndex_.find(std::make_pair(tableIndex, columnName));
            if (it == TableIndexAndColumnNameToTypeIndex_.end()) {
                typeIndex = ValueTypeToTypeIndex_[valueType];
                columnIdToTypeIndex[columnId] = UnschematizedTypeIndex;
            } else {
                typeIndex = columnIdToTypeIndex[columnId] = it->second;
            }
        }

        YT_VERIFY(typeIndex != UnknownTypeIndex && typeIndex != UnschematizedTypeIndex);
        return typeIndex;
    }
};

class TSchemalessValueWriter
{
public:
    TSchemalessValueWriter(
        const TWebJsonFormatConfigPtr& config,
        const std::vector<TTableSchema>& /* schemas */,
        IJsonConsumer* consumer)
        : FieldWeightLimit_(config->FieldWeightLimit)
        , Consumer_(consumer)
    {
        YT_VERIFY(config->ValueFormat == EWebJsonValueFormat::Schemaless);
    }

    void RegisterColumnNames(const TNameTablePtr& /* nameTable */)
    { }

    void ProcessRow(TUnversionedRow /* row */)
    { }

    void WriteValue(TStringBuf columnName, TUnversionedValue value)
    {
        switch (value.Type) {
            case EValueType::Any:
                Consumer_->OnNodeWeightLimited(
                    TStringBuf(value.Data.String, value.Length),
                    FieldWeightLimit_);
                break;
            case EValueType::String:
                Consumer_->OnStringScalarWeightLimited(
                    TStringBuf(value.Data.String, value.Length),
                    FieldWeightLimit_);
                break;
            default:
                WriteYsonValue(Consumer_, value);
        }
    }

    void WriteMetaInfo()
    { }

    static TJsonFormatConfigPtr GetJsonConfig(const TWebJsonFormatConfigPtr& /* webJsonConfig */)
    {
        auto config = New<TJsonFormatConfig>();
        config->Stringify = true;
        config->AnnotateWithTypes = true;
        return config;
    }

private:
    int FieldWeightLimit_;
    IJsonConsumer* Consumer_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TValueWriter>
class TWriterForWebJson
    : public ISchemalessFormatWriter
{
public:
    TWriterForWebJson(
        TNameTablePtr nameTable,
        IAsyncOutputStreamPtr output,
        TWebJsonColumnFilter columnFilter,
        const std::vector<TTableSchema>& schemas,
        TWebJsonFormatConfigPtr config);

    virtual bool Write(TRange<TUnversionedRow> rows) override;
    virtual TFuture<void> GetReadyEvent() override;
    virtual TBlob GetContext() const override;
    virtual i64 GetWrittenSize() const override;
    virtual TFuture<void> Close() override;

private:
    const TWebJsonFormatConfigPtr Config_;
    const TNameTablePtr NameTable_;
    const TNameTableReader NameTableReader_;

    TWrittenSizeAccountedOutputStream Output_;
    std::unique_ptr<IJsonConsumer> ResponseBuilder_;

    TWebJsonColumnFilter ColumnFilter_;
    THashMap<ui16, TString> AllColumnIdToName_;

    TValueWriter ValueWriter_;

    bool IncompleteAllColumnNames_ = false;
    bool IncompleteColumns_ = false;

    TError Error_;

private:
    bool TryRegisterColumn(ui16 columnId, TStringBuf columnName);
    bool SkipSystemColumn(TStringBuf columnName) const;

    void DoFlush(bool force);
    void DoWrite(TRange<TUnversionedRow> rows);
    void DoClose();
};

template <typename TValueWriter>
TWriterForWebJson<TValueWriter>::TWriterForWebJson(
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    TWebJsonColumnFilter columnFilter,
    const std::vector<TTableSchema>& schemas,
    TWebJsonFormatConfigPtr config)
    : Config_(std::move(config))
    , NameTable_(std::move(nameTable))
    , NameTableReader_(NameTable_)
    , Output_(CreateBufferedSyncAdapter(
        std::move(output),
        ESyncStreamAdapterStrategy::WaitFor,
        ContextBufferCapacity))
    , ResponseBuilder_(CreateJsonConsumer(
        &Output_,
        NYson::EYsonType::Node,
        TValueWriter::GetJsonConfig(Config_)))
    , ColumnFilter_(std::move(columnFilter))
    , ValueWriter_(Config_, schemas, ResponseBuilder_.get())
{
    try {
        ValueWriter_.RegisterColumnNames(NameTable_);
    } catch (const std::exception& ex) {
        Error_ = TError("Failed to register columns for web JSON writer") << ex;
    }

    ResponseBuilder_->OnBeginMap();
    ResponseBuilder_->OnKeyedItem("rows");
    ResponseBuilder_->OnBeginList();
}

template <typename TValueWriter>
bool TWriterForWebJson<TValueWriter>::Write(TRange<TUnversionedRow> rows)
{
    if (!Error_.IsOK()) {
        return false;
    }

    try {
        DoWrite(rows);
    } catch (const std::exception& ex) {
        Error_ = TError(ex);
        return false;
    }

    return true;
}

template <typename TValueWriter>
TFuture<void> TWriterForWebJson<TValueWriter>::GetReadyEvent()
{
    return MakeFuture(Error_);
}

template <typename TValueWriter>
TBlob TWriterForWebJson<TValueWriter>::GetContext() const
{
    return TBlob();
}

template <typename TValueWriter>
i64 TWriterForWebJson<TValueWriter>::GetWrittenSize() const
{
    return Output_.GetWrittenSize();
}

template <typename TValueWriter>
TFuture<void> TWriterForWebJson<TValueWriter>::Close()
{
    try {
        DoClose();
    } catch (const std::exception& exception) {
        Error_ = TError(exception);
    }

    return GetReadyEvent();
}

template <typename TValueWriter>
bool TWriterForWebJson<TValueWriter>::TryRegisterColumn(ui16 columnId, TStringBuf columnName)
{
    if (SkipSystemColumn(columnName)) {
        return false;
    }

    if (AllColumnIdToName_.size() < Config_->MaxAllColumnNamesCount) {
        AllColumnIdToName_[columnId] = columnName;
    } else if (!AllColumnIdToName_.contains(columnId)) {
        IncompleteAllColumnNames_ = true;
    }

    const auto result = ColumnFilter_.Accept(columnId, columnName);
    if (!result) {
        IncompleteColumns_ = true;
    }

    return result;
}

template <typename TValueWriter>
bool TWriterForWebJson<TValueWriter>::SkipSystemColumn(TStringBuf columnName) const
{
    if (!columnName.StartsWith(SystemColumnNamePrefix)) {
        return false;
    }
    return Config_->SkipSystemColumns;
}

template <typename TValueWriter>
void TWriterForWebJson<TValueWriter>::DoFlush(bool force)
{
    ResponseBuilder_->Flush();
    if (force) {
        Output_.Flush();
    }
}

template <typename TValueWriter>
void TWriterForWebJson<TValueWriter>::DoWrite(TRange<TUnversionedRow> rows)
{
    for (auto row : rows) {
        if (!row) {
            continue;
        }

        ResponseBuilder_->OnListItem();
        ResponseBuilder_->OnBeginMap();

        ValueWriter_.ProcessRow(row);

        for (auto value : row) {
            TStringBuf columnName;
            if (!NameTableReader_.TryGetName(value.Id, columnName)) {
                continue;
            }

            if (!TryRegisterColumn(value.Id, columnName)) {
                continue;
            }

            ResponseBuilder_->OnKeyedItem(columnName);
            ValueWriter_.WriteValue(columnName, value);
        }

        ResponseBuilder_->OnEndMap();

        DoFlush(false);
    }

    DoFlush(true);
}

template <typename TValueWriter>
void TWriterForWebJson<TValueWriter>::DoClose()
{
    if (Error_.IsOK()) {
        ResponseBuilder_->OnEndList();

        ResponseBuilder_->SetAnnotateWithTypesParameter(false);

        ResponseBuilder_->OnKeyedItem("incomplete_columns");
        ResponseBuilder_->OnBooleanScalar(IncompleteColumns_);

        ResponseBuilder_->OnKeyedItem("incomplete_all_column_names");
        ResponseBuilder_->OnBooleanScalar(IncompleteAllColumnNames_);

        ResponseBuilder_->OnKeyedItem("all_column_names");
        ResponseBuilder_->OnBeginList();

        std::vector<TStringBuf> allColumnNamesSorted;
        allColumnNamesSorted.reserve(AllColumnIdToName_.size());
        for (const auto& columnIdToName : AllColumnIdToName_) {
            allColumnNamesSorted.push_back(columnIdToName.second);
        }
        std::sort(allColumnNamesSorted.begin(), allColumnNamesSorted.end());

        for (const auto columnName : allColumnNamesSorted) {
            ResponseBuilder_->OnListItem();
            ResponseBuilder_->OnStringScalar(columnName);
        }

        ResponseBuilder_->OnEndList();

        ValueWriter_.WriteMetaInfo();

        ResponseBuilder_->OnEndMap();

        DoFlush(true);
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForWebJson(
    TWebJsonFormatConfigPtr config,
    TNameTablePtr nameTable,
    const std::vector<TTableSchema>& schemas,
    IAsyncOutputStreamPtr output)
{
    switch (config->ValueFormat) {
        case EWebJsonValueFormat::Schemaless:
            return New<TWriterForWebJson<TSchemalessValueWriter>>(
                std::move(nameTable),
                std::move(output),
                CreateWebJsonColumnFilter(config),
                schemas,
                std::move(config));
        case EWebJsonValueFormat::Yql:
            return New<TWriterForWebJson<TYqlValueWriter>>(
                std::move(nameTable),
                std::move(output),
                CreateWebJsonColumnFilter(config),
                schemas,
                std::move(config));

    }
    YT_ABORT();
}

ISchemalessFormatWriterPtr CreateWriterForWebJson(
    const NYTree::IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    const std::vector<TTableSchema>& schemas,
    IAsyncOutputStreamPtr output)
{
    return CreateWriterForWebJson(
        ConvertTo<TWebJsonFormatConfigPtr>(&attributes),
        std::move(nameTable),
        schemas,
        std::move(output));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
