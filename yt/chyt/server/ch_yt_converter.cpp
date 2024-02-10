#include "ch_yt_converter.h"

#include "std_helpers.h"
#include "config.h"
#include "data_type_boolean.h"
#include "format.h"
#include "columnar_conversion.h"

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/library/decimal/decimal.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/token_writer.h>
#include <yt/yt/core/yson/null_consumer.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>

#include <library/cpp/iterator/functools.h>

#include <util/generic/buffer.h>

#include <util/stream/buffer.h>

namespace NYT::NClickHouseServer {

using namespace NDecimal;
using namespace NLogging;
using namespace NTableClient;
using namespace NYson;

// Used only for YT_LOG_FATAL below.
static const TLogger Logger("CHYTConverter");

////////////////////////////////////////////////////////////////////////////////

// Anonymous namespace prevents ODR violation between CH->YT and YT->CH internal
// implementation classes.
namespace {

////////////////////////////////////////////////////////////////////////////////

//! Node in the conversion tree-like structure. Child nodes are saved by
//! std::unique_ptr<IConverter> in member fields of particular implementations.
struct IConverter
{
    //! Setup converter to work with given column.
    virtual void InitColumn(const DB::IColumn* column) = 0;

    //! This method fills given range with all values from given column. Note that it may
    //! fill not all of the unversioned values, e.g. when converting Nullable columns.
    virtual void FillValueRange(TMutableRange<TUnversionedValue> values) = 0;

    //! This method is used to fill "next" value from this column and pass it to the given
    //! YSON writer. If writer == nullptr, next value is simply ignored (which is useful
    //! when this converter is enclosed in Nullable converter).
    virtual void ExtractNextValueYson(TCheckedInDebugYsonTokenWriter* writer) = 0;

    virtual TLogicalTypePtr GetLogicalType() const = 0;
    virtual ~IConverter() = default;
};

using IConverterPtr = std::unique_ptr<IConverter>;

//////////////////////////////////////////////////////////////////////////////////

//! Value TypeId == Nothing is a special value that corresponds to YtBoolean.
template <DB::TypeIndex TypeId>
class TSimpleValueConverter
    : public IConverter
{
public:
    TSimpleValueConverter(DB::DataTypePtr dataType, ESimpleLogicalValueType simpleLogicalValueType)
        : DataType_(std::move(dataType))
        , LogicalType_(SimpleLogicalType(simpleLogicalValueType))
    { }

    void InitColumn(const DB::IColumn* column) override
    {
        Column_ = column;
        Data_ = Column_->getDataAt(0).data;
        ColumnString_ = dynamic_cast<const DB::ColumnString*>(Column_);
        CurrentValueIndex_ = 0;
    }

    void FillValueRange(TMutableRange<TUnversionedValue> values) override
    {
        YT_VERIFY(values.size() == Column_->size());

        for (int index = 0; index < static_cast<int>(values.size()); ++index) {
            #define XX(typeId, TChType, valueType, Accessor) \
                if constexpr (TypeId == typeId) { \
                    auto* typedData = reinterpret_cast<const TChType*>(Data_); \
                    values[index].Type = valueType; \
                    values[index].Data.Accessor = typedData[index]; \
                } else

            XX(DB::TypeIndex::Int8, DB::Int8, EValueType::Int64, Int64)
            XX(DB::TypeIndex::Int16, DB::Int16, EValueType::Int64, Int64)
            XX(DB::TypeIndex::Int32, DB::Int32, EValueType::Int64, Int64)
            XX(DB::TypeIndex::Int64, DB::Int64, EValueType::Int64, Int64)
            XX(DB::TypeIndex::UInt8, DB::UInt8, EValueType::Uint64, Uint64)
            XX(DB::TypeIndex::UInt16, DB::UInt16, EValueType::Uint64, Uint64)
            XX(DB::TypeIndex::UInt32, DB::UInt32, EValueType::Uint64, Uint64)
            XX(DB::TypeIndex::UInt64, DB::UInt64, EValueType::Uint64, Uint64)
            XX(DB::TypeIndex::Float32, DB::Float32, EValueType::Double, Double)
            XX(DB::TypeIndex::Float64, DB::Float64, EValueType::Double, Double)
            XX(DB::TypeIndex::Date, DB::UInt16, EValueType::Uint64, Uint64)
            XX(DB::TypeIndex::DateTime, DB::UInt32, EValueType::Uint64, Uint64)
            XX(DB::TypeIndex::Interval, DB::Int64, EValueType::Int64, Int64)
            /*else*/ if constexpr (TypeId == DB::TypeIndex::String) {
                YT_ASSERT(ColumnString_);
                values[index].Type = EValueType::String;
                // Use fully qualified method to prevent virtual call.
                auto stringRef = ColumnString_->DB::ColumnString::getDataAt(index);
                values[index].Data.String = stringRef.data;
                values[index].Length = stringRef.size;
            } else if constexpr (TypeId == DB::TypeIndex::Nothing) {
                // We need to validate UInt8 to be actually boolean.
                auto* typedData = reinterpret_cast<const DB::UInt8*>(Data_);
                values[index].Type = EValueType::Boolean;
                if (typedData[index] > 1) {
                    THROW_ERROR_EXCEPTION("Cannot convert value %v to YT boolean", typedData[index]);
                }
                values[index].Data.Boolean = typedData[index];
            } else {
                THROW_ERROR_EXCEPTION(
                    "Conversion of ClickHouse type %v to YT type system is not supported",
                    DataType_->getName());
            }

            #undef XX
        }
    }

    void ExtractNextValueYson(TCheckedInDebugYsonTokenWriter* writer) override
    {
        YT_ASSERT(CurrentValueIndex_ < std::ssize(*Column_));

        if (!writer) {
            ++CurrentValueIndex_;
            return;
        }

        #define XX(typeId, TChType, method) \
            if constexpr (TypeId == typeId) { \
                auto* typedData = reinterpret_cast<const TChType*>(Data_); \
                writer->method(typedData[CurrentValueIndex_]); \
            } else

        XX(DB::TypeIndex::Int8, DB::Int8, WriteBinaryInt64)
        XX(DB::TypeIndex::Int16, DB::Int16, WriteBinaryInt64)
        XX(DB::TypeIndex::Int32, DB::Int32, WriteBinaryInt64)
        XX(DB::TypeIndex::Int64, DB::Int64, WriteBinaryInt64)
        XX(DB::TypeIndex::UInt8, DB::UInt8, WriteBinaryUint64)
        XX(DB::TypeIndex::UInt16, DB::UInt16, WriteBinaryUint64)
        XX(DB::TypeIndex::UInt32, DB::UInt32, WriteBinaryUint64)
        XX(DB::TypeIndex::UInt64, DB::UInt64, WriteBinaryUint64)
        XX(DB::TypeIndex::Float32, DB::Float32, WriteBinaryDouble)
        XX(DB::TypeIndex::Float64, DB::Float64, WriteBinaryDouble)
        XX(DB::TypeIndex::Date, DB::UInt16, WriteBinaryUint64)
        XX(DB::TypeIndex::DateTime, DB::UInt32, WriteBinaryUint64)
        XX(DB::TypeIndex::Interval, DB::Int64, WriteBinaryInt64)
        /*else*/ if constexpr (TypeId == DB::TypeIndex::String) {
            YT_ASSERT(ColumnString_);
            // Use fully qualified method to prevent virtual call.
            auto stringRef = ColumnString_->DB::ColumnString::getDataAt(CurrentValueIndex_);
            writer->WriteBinaryString(TStringBuf(stringRef.data, stringRef.size));
        } else if constexpr (TypeId == DB::TypeIndex::Nothing) {
            // We need to validate UInt8 to be actually boolean.
            auto* typedData = reinterpret_cast<const DB::UInt8*>(Data_);
            if (typedData[CurrentValueIndex_] > 1) {
                THROW_ERROR_EXCEPTION("Cannot convert value %v to YT boolean", typedData[CurrentValueIndex_]);
            }
            writer->WriteBinaryBoolean(typedData[CurrentValueIndex_]);
        } else {
            THROW_ERROR_EXCEPTION(
                "Conversion of ClickHouse type %v to YT type system is not supported",
                DataType_->getName());
        }

        ++CurrentValueIndex_;

        #undef XX
    }

    TLogicalTypePtr GetLogicalType() const override
    {
        return LogicalType_;
    }

private:
    const DB::IColumn* Column_;
    const char* Data_ = nullptr;
    const DB::ColumnString* ColumnString_ = nullptr;
    i64 CurrentValueIndex_ = 0;

    DB::DataTypePtr DataType_;
    TLogicalTypePtr LogicalType_;
};

////////////////////////////////////////////////////////////////////////////////

class TNullableConverter
    : public IConverter
{
public:
    explicit TNullableConverter(IConverterPtr underlyingConverter)
        : UnderlyingConverter_(std::move(underlyingConverter))
    { }

    void InitColumn(const DB::IColumn* column) override
    {
        YT_VERIFY(column->isNullable());
        auto* columnNullable = DB::checkAndGetColumn<DB::ColumnNullable>(column);
        YT_VERIFY(columnNullable);
        NullColumn_ = &columnNullable->getNullMapColumn();
        NullData_ = &NullColumn_->getData();
        UnderlyingConverter_->InitColumn(&columnNullable->getNestedColumn());
        CurrentValueIndex_ = 0;
    }

    void FillValueRange(TMutableRange<TUnversionedValue> values) override
    {
        UnderlyingConverter_->FillValueRange(values);

        YT_VERIFY(NullData_->size() == values.size());

        for (int index = 0; index < static_cast<int>(NullData_->size()); ++index) {
            if ((*NullData_)[index]) {
                values[index] = MakeUnversionedNullValue();
            }
        }
    }

    void ExtractNextValueYson(TCheckedInDebugYsonTokenWriter* writer) override
    {
        if (!writer) {
            // Technically this can't happen since Nullable can't be enclosed in Nullable in CH.
            ++CurrentValueIndex_;
            UnderlyingConverter_->ExtractNextValueYson(nullptr);
            return;
        }

        if ((*NullData_)[CurrentValueIndex_]) {
            writer->WriteEntity();
            UnderlyingConverter_->ExtractNextValueYson(nullptr);
        } else {
            UnderlyingConverter_->ExtractNextValueYson(writer);
        }
        ++CurrentValueIndex_;
    }

    TLogicalTypePtr GetLogicalType() const override
    {
        return OptionalLogicalType(UnderlyingConverter_->GetLogicalType());
    }

private:
    const DB::ColumnUInt8* NullColumn_ = nullptr;
    const DB::ColumnUInt8::Container* NullData_ = nullptr;
    i64 CurrentValueIndex_ = 0;
    const IConverterPtr UnderlyingConverter_;
};

//////////////////////////////////////////////////////////////////////////////////

class TArrayConverter
    : public IConverter
{
public:
    explicit TArrayConverter(IConverterPtr underlyingConverter)
        : UnderlyingConverter_(std::move(underlyingConverter))
    { }

    void InitColumn(const DB::IColumn* column) override
    {
        auto* columnArray = DB::checkAndGetColumn<DB::ColumnArray>(column);
        YT_VERIFY(columnArray);
        Offsets_ = &columnArray->getOffsets();
        CurrentValueIndex_ = 0;
        UnderlyingConverter_->InitColumn(&columnArray->getData());
    }

    void FillValueRange(TMutableRange<TUnversionedValue> /*values*/) override
    {
        // We should not get here.
        YT_ABORT();
    }

    void ExtractNextValueYson(TCheckedInDebugYsonTokenWriter* writer) override
    {
        auto beginOffset = CurrentValueIndex_ > 0 ? (*Offsets_)[CurrentValueIndex_ - 1] : 0;
        auto endOffset = (*Offsets_)[CurrentValueIndex_];

        if (!writer) {
            // Technically this can't happen since Array can't be enclosed in Nullable in CH.
            ++CurrentValueIndex_;
            while (beginOffset < endOffset) {
                UnderlyingConverter_->ExtractNextValueYson(nullptr);
                ++beginOffset;
            }
            return;
        }

        writer->WriteBeginList();
        while (beginOffset < endOffset) {
            UnderlyingConverter_->ExtractNextValueYson(writer);
            writer->WriteItemSeparator();
            ++beginOffset;
        }
        writer->WriteEndList();

        ++CurrentValueIndex_;
    }

    TLogicalTypePtr GetLogicalType() const override
    {
        return ListLogicalType(UnderlyingConverter_->GetLogicalType());
    }

private:
    const DB::ColumnArray::Offsets* Offsets_ = nullptr;
    i64 CurrentValueIndex_ = 0;
    const IConverterPtr UnderlyingConverter_;
};

////////////////////////////////////////////////////////////////////////////////

class TTupleConverter
    : public IConverter
{
public:
    explicit TTupleConverter(std::vector<IConverterPtr> underlyingConverters, std::optional<std::vector<TString>> elementNames)
        : UnderlyingConverters_(std::move(underlyingConverters))
        , ElementNames_(std::move(elementNames))
    {
        YT_VERIFY(!ElementNames_ || ElementNames_->size() == UnderlyingConverters_.size());
    }

    void InitColumn(const DB::IColumn* column) override
    {
        auto* columnTuple = DB::checkAndGetColumn<DB::ColumnTuple>(column);
        YT_VERIFY(columnTuple);
        YT_VERIFY(columnTuple->getColumns().size() == UnderlyingConverters_.size());
        for (const auto& [nestedColumn, underlyingConverter] : Zip(columnTuple->getColumns(), UnderlyingConverters_)) {
            underlyingConverter->InitColumn(&*nestedColumn);
        }
    }

    void FillValueRange(TMutableRange<TUnversionedValue> /*values*/) override
    {
        // We should not get here.
        YT_ABORT();
    }

    void ExtractNextValueYson(TCheckedInDebugYsonTokenWriter* writer) override
    {
        if (!writer) {
            for (const auto& underlyingConverter : UnderlyingConverters_) {
                underlyingConverter->ExtractNextValueYson(nullptr);
            }
            return;
        }

        writer->WriteBeginList();
        for (const auto& underlyingConverter : UnderlyingConverters_) {
            underlyingConverter->ExtractNextValueYson(writer);
            writer->WriteItemSeparator();
        }
        writer->WriteEndList();
    }

    TLogicalTypePtr GetLogicalType() const override
    {
        if (!ElementNames_) {
            std::vector<TLogicalTypePtr> underlyingLogicalTypes;
            underlyingLogicalTypes.reserve(UnderlyingConverters_.size());
            for (const auto& underlyingConverter : UnderlyingConverters_) {
                underlyingLogicalTypes.emplace_back(underlyingConverter->GetLogicalType());
            }
            return TupleLogicalType(std::move(underlyingLogicalTypes));
        } else {
            std::vector<TStructField> structFields;
            structFields.reserve(UnderlyingConverters_.size());
            for (const auto& [underlyingConverter, elementName] : Zip(UnderlyingConverters_, *ElementNames_)) {
                structFields.push_back({elementName, underlyingConverter->GetLogicalType()});
            }
            return StructLogicalType(std::move(structFields));
        }
    }

private:
    const std::vector<IConverterPtr> UnderlyingConverters_;
    const std::optional<std::vector<TString>> ElementNames_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlyingIntegerType>
class TDecimalConverter
    : public IConverter
{
public:
    static_assert(std::is_same_v<TUnderlyingIntegerType, DB::Int32>
        || std::is_same_v<TUnderlyingIntegerType, DB::Int64>
        || std::is_same_v<TUnderlyingIntegerType, DB::Int128>);

    using TClickHouseDecimal = DB::Decimal<TUnderlyingIntegerType>;
    using TDecimalColumn = DB::ColumnDecimal<TClickHouseDecimal>;
    static constexpr i64 DecimalSize = sizeof(TUnderlyingIntegerType);

    TDecimalConverter(int precision, int scale)
        : Precision_(precision)
        , Scale_(scale)
    { }

    void InitColumn(const DB::IColumn* column) override
    {
        Column_ = DB::checkAndGetColumn<TDecimalColumn>(column);
        YT_VERIFY(Column_);
        CurrentValueIndex_ = 0;
    }

    void FillValueRange(TMutableRange<TUnversionedValue> values) override
    {
        YT_VERIFY(values.size() == Column_->size());

        Buffer_.resize(values.size() * DecimalSize);

        const char* data = Column_->template getRawDataBegin<DecimalSize>();

        for (int index = 0; index < std::ssize(values); ++index) {
            const char* chValue = data + DecimalSize * index;
            char* ytValue = Buffer_.begin() + DecimalSize * index;
            DoConvertDecimal(ytValue, chValue);

            values[index].Type = EValueType::String;
            values[index].Data.String = ytValue;
            values[index].Length = DecimalSize;
        }
    }

    void ExtractNextValueYson(TCheckedInDebugYsonTokenWriter* writer) override
    {
        YT_VERIFY(CurrentValueIndex_ < static_cast<int>(Column_->size()));

        if (writer) {
            const char* data = Column_->template getRawDataBegin<DecimalSize>();
            const char* chValue = data + DecimalSize * CurrentValueIndex_;

            char ytValue[DecimalSize];
            DoConvertDecimal(ytValue, chValue);

            writer->WriteBinaryString(TStringBuf(ytValue, DecimalSize));
        }
        ++CurrentValueIndex_;
    }

    TLogicalTypePtr GetLogicalType() const override
    {
        return DecimalLogicalType(Precision_, Scale_);
    }

private:
    int Precision_;
    int Scale_;

    const TDecimalColumn* Column_ = nullptr;
    i64 CurrentValueIndex_ = 0;
    // Buffer to store decimals in YT representation.
    TString Buffer_;

    void DoConvertDecimal(char* ytValue, const char* chValue)
    {
        if constexpr (std::is_same_v<TUnderlyingIntegerType, DB::Int32>) {
            i32 value;
            memcpy(&value, chValue, DecimalSize);
            TDecimal::WriteBinary32(Precision_, value, ytValue, DecimalSize);
        } else if constexpr (std::is_same_v<TUnderlyingIntegerType, DB::Int64>) {
            i64 value;
            memcpy(&value, chValue, DecimalSize);
            TDecimal::WriteBinary64(Precision_, value, ytValue, DecimalSize);
        } else if constexpr (std::is_same_v<TUnderlyingIntegerType, DB::Int128>) {
            TDecimal::TValue128 value;
            memcpy(&value, chValue, DecimalSize);
            TDecimal::WriteBinary128(Precision_, value, ytValue, DecimalSize);
        } else {
            YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TCHYTConverter::TImpl
{
public:
    TImpl(DB::DataTypePtr dataType, TCompositeSettingsPtr settings)
        : DataType_(std::move(dataType))
        , Settings_(std::move(settings))
        , RootConverter_(CreateConverter(DataType_))
    { }

    TLogicalTypePtr GetLogicalType() const
    {
        return RootConverter_->GetLogicalType();
    }

    TUnversionedValueRange ConvertColumnToUnversionedValues(const DB::ColumnPtr& column)
    {
        // Note that this assignment sets all value ids to zero.
        CurrentValues_.assign(column->size(), MakeUnversionedSentinelValue(EValueType::TheBottom));
        Buffer_.Clear();
        // We save current column to be able to prolong its lifetime until next call of
        // ConvertColumnToUnversionedValues. This allows us to form string-like unversioned values
        // pointing directly to the input column.
        // TODO(dakovalkov): support const and low-cardinality columns without conversion to full column.
        CurrentColumn_ = column->convertToFullIfNeeded();

        RootConverter_->InitColumn(CurrentColumn_.get());

        auto logicalType = RootConverter_->GetLogicalType();
        bool isDecimal = (logicalType->GetMetatype() == ELogicalMetatype::Decimal ||
            (logicalType->GetMetatype() == ELogicalMetatype::Optional && logicalType->GetElement()->GetMetatype() == ELogicalMetatype::Decimal));

        if (IsV1Type(logicalType) || isDecimal) {
            RootConverter_->FillValueRange(CurrentValues_);
        } else {
            TBufferOutput output(Buffer_);
            TZeroCopyOutputStreamWriter streamWriter(&output);
            std::vector<size_t> offsets = {0};
            for (size_t index = 0; index < column->size(); ++index) {
                TCheckedInDebugYsonTokenWriter writer(&streamWriter);
                RootConverter_->ExtractNextValueYson(&writer);
                offsets.emplace_back(streamWriter.GetTotalWrittenSize());
            }
            for (size_t index = 0; index < column->size(); ++index) {
                CurrentValues_[index].Type = EValueType::Composite;
                CurrentValues_[index].Data.String = Buffer_.data() + offsets[index];
                CurrentValues_[index].Length = offsets[index + 1] - offsets[index];
            }
            #ifndef NDEBUG

            // Validate that we formed valid YSONs (I know that TCheckedInDebugYsonTokenWriter
            // already does that, but nevertheless let's make sure I didn't mess up with offsets).
            for (size_t index = 0; index < column->size(); ++index) {
                try {
                    TNullYsonConsumer consumer;
                    TMemoryInput input(CurrentValues_[index].Data.String, CurrentValues_[index].Length);
                    TYsonInput ysonInput(&input);
                    ParseYson(ysonInput, &consumer);
                } catch (const std::exception& ex) {
                    YT_LOG_FATAL(ex, "Error while converting value %v", index);
                }
            }

            #endif
        }

        #ifndef NDEBUG

        // Assert that we did not forget to fill any of the values.
        for (const auto& value : CurrentValues_) {
            YT_VERIFY(value.Type != EValueType::TheBottom);
        }

        #endif

        return CurrentValues_;
    }

private:
    const DB::DataTypePtr DataType_;
    TCompositeSettingsPtr Settings_;

    const IConverterPtr RootConverter_;

    DB::ColumnPtr CurrentColumn_;
    std::vector<TUnversionedValue> CurrentValues_;
    TBuffer Buffer_;

    IConverterPtr CreateSimpleValueConverter(const DB::DataTypePtr& dataType)
    {
        switch (dataType->getTypeId()) {
            #define XX(typeId, simpleLogicalValueType) \
                case DB::TypeIndex::typeId: \
                    return std::make_unique<TSimpleValueConverter<DB::TypeIndex::typeId>>( \
                        dataType, \
                        ESimpleLogicalValueType::simpleLogicalValueType);

            XX(Int8, Int8)
            XX(Int16, Int16)
            XX(Int32, Int32)
            XX(Int64, Int64)
            XX(UInt16, Uint16)
            XX(UInt32, Uint32)
            XX(UInt64, Uint64)
            XX(Float32, Float)
            XX(Float64, Double)
            XX(String, String)
            XX(Date, Date)
            XX(DateTime, Datetime)
            XX(Interval, Interval)

            case DB::TypeIndex::UInt8:
                if (dataType->getCustomName() && dataType->getCustomName()->getName() == "YtBoolean") {
                    // Nothing is a special value standing for YT boolean for simplicity.
                    return std::make_unique<TSimpleValueConverter<DB::TypeIndex::Nothing>>(
                        dataType,
                        ESimpleLogicalValueType::Boolean);
                } else {
                    return std::make_unique<TSimpleValueConverter<DB::TypeIndex::UInt8>>(
                        dataType,
                        ESimpleLogicalValueType::Uint8);
                }

            #undef XX

            default:
                YT_ABORT();
        }
    }

    IConverterPtr CreateNullableConverter(const DB::DataTypePtr& dataType)
    {
        auto dataTypeNullable = dynamic_pointer_cast<const DB::DataTypeNullable>(dataType);
        YT_VERIFY(dataTypeNullable);
        auto underlyingConverter = CreateConverter(dataTypeNullable->getNestedType());
        return std::make_unique<TNullableConverter>(std::move(underlyingConverter));
    }

    IConverterPtr CreateArrayConverter(const DB::DataTypePtr& dataType)
    {
        auto dataTypeArray = dynamic_pointer_cast<const DB::DataTypeArray>(dataType);
        YT_VERIFY(dataTypeArray);
        auto underlyingConverter = CreateConverter(dataTypeArray->getNestedType());
        return std::make_unique<TArrayConverter>(std::move(underlyingConverter));
    }

    IConverterPtr CreateTupleConverter(const DB::DataTypePtr& dataType)
    {
        auto dataTypeTuple = dynamic_pointer_cast<const DB::DataTypeTuple>(dataType);
        YT_VERIFY(dataTypeTuple);
        std::vector<IConverterPtr> underlyingConverters;
        auto elementNames = ToVectorString(dataTypeTuple->getElementNames());
        underlyingConverters.reserve(dataTypeTuple->getElements().size());
        for (const auto& elementDataType : dataTypeTuple->getElements()) {
            underlyingConverters.emplace_back(CreateConverter(elementDataType));
        }
        return std::make_unique<TTupleConverter>(
            std::move(underlyingConverters),
            dataTypeTuple->haveExplicitNames() ? std::make_optional(elementNames) : std::nullopt);
    }

    IConverterPtr CreateDecimalConverter(const DB::DataTypePtr& dataType)
    {
        int precision = DB::getDecimalPrecision(*dataType);
        int scale = DB::getDecimalScale(*dataType);

        if (precision > 35) {
            THROW_ERROR_EXCEPTION("ClickHouse type %v is not representable as YT type: "
                "maximum decimal precision in YT is 35",
                DataType_->getName())
                << TErrorAttribute("docs", "https://ytsaurus.tech/docs/en/user-guide/storage/data-types#schema_decimal");
        }

        switch (dataType->getTypeId()) {
            case DB::TypeIndex::Decimal32:
                return std::make_unique<TDecimalConverter<DB::Int32>>(precision, scale);
            case DB::TypeIndex::Decimal64:
                return std::make_unique<TDecimalConverter<DB::Int64>>(precision, scale);
            case DB::TypeIndex::Decimal128:
                return std::make_unique<TDecimalConverter<DB::Int128>>(precision, scale);
            case DB::TypeIndex::Decimal256:
                // Decimal256 has precision in range [39:76], which is more than
                // maximum supported precision in YT decimal type (35).
                // We should not get here because precision has already been checked.
                YT_ABORT();
            default:
                YT_ABORT();
        }
    }

    IConverterPtr CreateConverter(const DB::DataTypePtr& dataType)
    {
        switch (dataType->getTypeId()) {
            case DB::TypeIndex::Int8:
            case DB::TypeIndex::Int16:
            case DB::TypeIndex::Int32:
            case DB::TypeIndex::Int64:
            case DB::TypeIndex::UInt8:
            case DB::TypeIndex::UInt16:
            case DB::TypeIndex::UInt32:
            case DB::TypeIndex::UInt64:
            case DB::TypeIndex::Float32:
            case DB::TypeIndex::Float64:
            case DB::TypeIndex::String:
            case DB::TypeIndex::Date:
            case DB::TypeIndex::DateTime:
            case DB::TypeIndex::Interval:
                return CreateSimpleValueConverter(dataType);
            case DB::TypeIndex::Nullable:
                return CreateNullableConverter(dataType);
            case DB::TypeIndex::Array:
                return CreateArrayConverter(dataType);
            case DB::TypeIndex::Tuple:
                return CreateTupleConverter(dataType);
            case DB::TypeIndex::Decimal32:
            case DB::TypeIndex::Decimal64:
            case DB::TypeIndex::Decimal128:
            case DB::TypeIndex::Decimal256:
                return CreateDecimalConverter(dataType);
            default:
                THROW_ERROR_EXCEPTION(
                    "Conversion of ClickHouse type %v to YT type system is not supported",
                    DataType_->getName());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TCHYTConverter::TCHYTConverter(
    DB::DataTypePtr dataType,
    TCompositeSettingsPtr settings)
    : Impl_(std::make_unique<TImpl>(std::move(dataType), std::move(settings)))
{ }

TLogicalTypePtr TCHYTConverter::GetLogicalType() const
{
    return Impl_->GetLogicalType();
}

TUnversionedValueRange TCHYTConverter::ConvertColumnToUnversionedValues(
    const DB::ColumnPtr& column)
{
    return Impl_->ConvertColumnToUnversionedValues(column);
}

TCHYTConverter::~TCHYTConverter() = default;

TCHYTConverter::TCHYTConverter(
    TCHYTConverter&& other)
    : Impl_(std::move(other.Impl_))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
