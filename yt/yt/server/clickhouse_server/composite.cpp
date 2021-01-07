#include "composite.h"

#include "config.h"

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/token_writer.h>

#include <Core/Types.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

template <typename... Args>
[[noreturn]] void ThrowConversionError(const TComplexTypeFieldDescriptor& descriptor, const Args&... args)
{
    THROW_ERROR_EXCEPTION(
        "Composite type to ClickHouse conversion error while converting %Qv field",
        descriptor.GetDescription())
            << TError(args...);
}

template <class TColumn>
TColumn* CheckAndGetMutable(const DB::MutableColumnPtr& column)
{
    auto* typedColumn = DB::checkAndGetColumn<TColumn>(column.get());
    return const_cast<TColumn*>(typedColumn);
}

////////////////////////////////////////////////////////////////////////////////

//! Node in the conversion tree-like structure. Child nodes are saved by
//! std::unique_ptr<IConverter> in member fields of particular implementations.
struct IConverter
{
    virtual void Consume(TYsonPullParserCursor* cursor) = 0;
    virtual void ConsumeNull() = 0;
    virtual DB::ColumnPtr FlushColumn() = 0;
    virtual DB::DataTypePtr GetDataType() const = 0;
    virtual ~IConverter() = default;
};

using IConverterPtr = std::unique_ptr<IConverter>;

////////////////////////////////////////////////////////////////////////////////

class TRawYsonToStringConverter
    : public IConverter
{
public:
    TRawYsonToStringConverter(const TComplexTypeFieldDescriptor& /* descriptor */, const TCompositeSettingsPtr& settings)
        : Column_(DB::ColumnString::create())
        , YsonOutput_(YsonBuffer_)
        , YsonWriter_(&YsonOutput_, settings->DefaultYsonFormat)
    { }

    virtual void Consume(TYsonPullParserCursor* cursor)
    {
        auto& offsets = Column_->getOffsets();
        auto& chars = Column_->getChars();

        cursor->TransferComplexValue(&YsonWriter_);
        YsonWriter_.Flush();

        chars.insert(chars.end(), YsonBuffer_.begin(), YsonBuffer_.end());
        chars.push_back('\x0');
        offsets.push_back(chars.size());
        YsonBuffer_.clear();
    }

    virtual void ConsumeNull()
    {
        // If somebody called ConsumeNull() here, we are probably inside Nullable
        // column, so the exact value here does not matter.
        Column_->insertDefault();
    }

    virtual DB::ColumnPtr FlushColumn() override
    {
        return std::move(Column_);
    }

    virtual DB::DataTypePtr GetDataType() const override
    {
        return std::make_shared<DB::DataTypeString>();
    }

private:
    DB::ColumnString::MutablePtr Column_;
    TString YsonBuffer_;
    TStringOutput YsonOutput_;
    TYsonWriter YsonWriter_;
};

////////////////////////////////////////////////////////////////////////////////

template <ESimpleLogicalValueType valueType, class TCppType, class TColumn>
class TSimpleValueConverter
    : public IConverter
{
public:
    TSimpleValueConverter(TComplexTypeFieldDescriptor descriptor, DB::DataTypePtr dataType)
        : Descriptor_(std::move(descriptor))
        , DataType_(std::move(dataType))
        , Column_(DataType_->createColumn())
    { }

    virtual void Consume(TYsonPullParserCursor* cursor) override
    {
        auto ysonItem = cursor->GetCurrent();

        // NB: even though (at least) one of the downcasts is semantically incorrect,
        // proper choice of valueType guarantees that we will never dereference an incorrect
        // pointer, thus no UB happens. On the other hand, we become able to access public
        // non-interface methods specific to particular column implementation.
        auto* typedVectorColumn = static_cast<DB::ColumnVector<TCppType>*>(Column_.get());
        auto* typedStringColumn = static_cast<DB::ColumnString*>(Column_.get());
        auto* typedNothingColumn = static_cast<DB::ColumnNothing*>(Column_.get());

        if constexpr (
            valueType == ESimpleLogicalValueType::Int8 ||
            valueType == ESimpleLogicalValueType::Int16 ||
            valueType == ESimpleLogicalValueType::Int32 ||
            valueType == ESimpleLogicalValueType::Int64 ||
            valueType == ESimpleLogicalValueType::Interval)
        {
            typedVectorColumn->insertValue(ysonItem.UncheckedAsInt64());
        } else if constexpr (
            valueType == ESimpleLogicalValueType::Uint8 ||
            valueType == ESimpleLogicalValueType::Uint16 ||
            valueType == ESimpleLogicalValueType::Uint32 ||
            valueType == ESimpleLogicalValueType::Uint64 ||
            valueType == ESimpleLogicalValueType::Date ||
            valueType == ESimpleLogicalValueType::Datetime ||
            valueType == ESimpleLogicalValueType::Timestamp)
        {
            typedVectorColumn->insertValue(ysonItem.UncheckedAsUint64());
        } else if constexpr (
            valueType == ESimpleLogicalValueType::Float ||
            valueType == ESimpleLogicalValueType::Double)
        {
            typedVectorColumn->insertValue(ysonItem.UncheckedAsDouble());
        } else if constexpr (valueType == ESimpleLogicalValueType::Boolean)
        {
            typedVectorColumn->insertValue(ysonItem.UncheckedAsBoolean());
        } else if constexpr (
            valueType == ESimpleLogicalValueType::String ||
            valueType == ESimpleLogicalValueType::Utf8)
        {
            auto data = ysonItem.UncheckedAsString();
            typedStringColumn->insertData(data.data(), data.size());
        } else if constexpr (valueType == ESimpleLogicalValueType::Void) {
            YT_VERIFY(ysonItem.GetType() == EYsonItemType::EntityValue);
            typedNothingColumn->insertDefault();
        } else {
            YT_ABORT();
        }
        cursor->Next();
    }

    virtual void ConsumeNull() override
    {
        // If somebody called ConsumeNull() here, we are probably inside Nullable
        // column, so the exact value here does not matter.
        Column_->insertDefault();
    }

    virtual DB::ColumnPtr FlushColumn() override
    {
        return std::move(Column_);
    }

    virtual DB::DataTypePtr GetDataType() const override
    {
        return DataType_;
    }

private:
    TComplexTypeFieldDescriptor Descriptor_;
    DB::DataTypePtr DataType_;
    DB::IColumn::MutablePtr Column_;
};

////////////////////////////////////////////////////////////////////////////////

class TOptionalConverter
    : public IConverter
{
public:
    TOptionalConverter(IConverterPtr underlyingConverter, int nestingLevel)
        : UnderlyingConverter_(std::move(underlyingConverter))
        , NestingLevel_(nestingLevel)
    {
        // Tuples and arrays cannot be inside Nullable() in ClickHouse.
        // Also note that all non-simple types are represented as tuples and arrays.
        // Both DB::makeNullable are silently returning original argument if they see
        // something that is already nullable.
        if (UnderlyingConverter_->GetDataType()->canBeInsideNullable()) {
            NullColumn_ = DB::ColumnVector<DB::UInt8>::create();
        }
    }

    virtual void Consume(TYsonPullParserCursor* cursor) override
    {
        int outerOptionalsFound = 0;
        while (cursor->GetCurrent().GetType() == EYsonItemType::BeginList && outerOptionalsFound < NestingLevel_ - 1) {
            ++outerOptionalsFound;
            cursor->Next();
        }
        if (outerOptionalsFound < NestingLevel_ - 1) {
            // This have to be entity of some level.
            YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::EntityValue);
            ConsumeNull();
            cursor->Next();
        } else {
            YT_VERIFY(outerOptionalsFound == NestingLevel_ - 1);
            // It may either be entity or a representation of underlying non-optional type.
            if (cursor->GetCurrent().GetType() == EYsonItemType::EntityValue) {
                ConsumeNull();
                cursor->Next();
            } else {
                if (NullColumn_) {
                    NullColumn_->insertValue(0);
                }
                UnderlyingConverter_->Consume(cursor);
            }
        }
        while (outerOptionalsFound--) {
            YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::EndList);
            cursor->Next();
        }
    }

    virtual void ConsumeNull() override
    {
        if (NullColumn_) {
            NullColumn_->insertValue(1);
        }
        UnderlyingConverter_->ConsumeNull();
    }

    virtual DB::ColumnPtr FlushColumn() override
    {
        auto underlyingColumn = UnderlyingConverter_->FlushColumn();

        if (NullColumn_) {
            // Pass ownership to ColumnNullable and make sure it won't be COWed.
            DB::ColumnVector<DB::UInt8>::Ptr nullColumn = std::move(NullColumn_);
            YT_VERIFY(nullColumn->use_count() == 1);
            return DB::ColumnNullable::create(underlyingColumn, nullColumn);
        } else {
            return underlyingColumn;
        }
    }

    virtual DB::DataTypePtr GetDataType() const override
    {
        if (UnderlyingConverter_->GetDataType()->canBeInsideNullable()) {
            return std::make_shared<DB::DataTypeNullable>(UnderlyingConverter_->GetDataType());
        } else {
            return UnderlyingConverter_->GetDataType();
        }
    }

private:
    IConverterPtr UnderlyingConverter_;
    int NestingLevel_;
    DB::ColumnVector<DB::UInt8>::MutablePtr NullColumn_;
};

////////////////////////////////////////////////////////////////////////////////

class TListConverter
    : public IConverter
{
public:
    TListConverter(IConverterPtr underlyingConverter)
        : UnderlyingConverter_(std::move(underlyingConverter))
        , ColumnOffsets_(DB::ColumnVector<ui64>::create())
    { }

    virtual void Consume(TYsonPullParserCursor* cursor) override
    {
        YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::BeginList);
        cursor->Next();

        while (cursor->GetCurrent().GetType() != EYsonItemType::EndList && cursor->GetCurrent().GetType() != EYsonItemType::EndOfStream) {
            UnderlyingConverter_->Consume(cursor);
            ++ItemCount_;
        }

        YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::EndList);
        cursor->Next();

        ColumnOffsets_->insertValue(ItemCount_);
    }

    virtual void ConsumeNull() override
    {
        // Null is represented as an empty array.
        ColumnOffsets_->insertValue(ItemCount_);
    }

    virtual DB::ColumnPtr FlushColumn() override
    {
        DB::ColumnVector<ui64>::Ptr columnOffsets = std::move(ColumnOffsets_);
        return DB::ColumnArray::create(UnderlyingConverter_->FlushColumn(), columnOffsets);
    }

    virtual DB::DataTypePtr GetDataType() const override
    {
        return std::make_shared<DB::DataTypeArray>(UnderlyingConverter_->GetDataType());
    }

private:
    IConverterPtr UnderlyingConverter_;
    DB::ColumnVector<ui64>::MutablePtr ColumnOffsets_;
    ui64 ItemCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TDictConverter
    : public IConverter
{
public:
    TDictConverter(IConverterPtr keyConverter, IConverterPtr valueConverter)
        : KeyConverter_(std::move(keyConverter))
        , ValueConverter_(std::move(valueConverter))
        , ColumnOffsets_(DB::ColumnVector<ui64>::create())
    { }

    virtual void Consume(TYsonPullParserCursor* cursor) override
    {
        YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::BeginList);
        cursor->Next();

        while (cursor->GetCurrent().GetType() != EYsonItemType::EndList && cursor->GetCurrent().GetType() != EYsonItemType::EndOfStream) {
            YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::BeginList);
            cursor->Next();
            KeyConverter_->Consume(cursor);
            ValueConverter_->Consume(cursor);
            YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::EndList);
            cursor->Next();
            ++ItemCount_;
        }

        YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::EndList);
        cursor->Next();

        ColumnOffsets_->insertValue(ItemCount_);
    }

    virtual void ConsumeNull() override
    {
        // Null is represented as an empty array.
        ColumnOffsets_->insertValue(ItemCount_);
    }

    virtual DB::ColumnPtr FlushColumn() override
    {
        DB::ColumnVector<ui64>::Ptr columnOffsets = std::move(ColumnOffsets_);
        auto keyColumn = KeyConverter_->FlushColumn()->assumeMutable();
        auto valueColumn = ValueConverter_->FlushColumn()->assumeMutable();
        std::vector<DB::IColumn::MutablePtr> columns;
        columns.emplace_back(std::move(keyColumn));
        columns.emplace_back(std::move(valueColumn));
        auto columnTuple = DB::ColumnTuple::create(std::move(columns));
        return DB::ColumnArray::create(std::move(columnTuple), columnOffsets);
    }

    virtual DB::DataTypePtr GetDataType() const override
    {
        auto tupleDataType = std::make_shared<DB::DataTypeTuple>(
            std::vector<DB::DataTypePtr>{KeyConverter_->GetDataType(), ValueConverter_->GetDataType()},
            std::vector<std::string>{"key", "value"});

        return std::make_shared<DB::DataTypeArray>(std::move(tupleDataType));
    }

private:
    IConverterPtr KeyConverter_;
    IConverterPtr ValueConverter_;
    DB::ColumnVector<ui64>::MutablePtr ColumnOffsets_;
    ui64 ItemCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TTupleConverter
    : public IConverter
{
public:
    TTupleConverter(std::vector<IConverterPtr> itemConverters)
        : ItemConverters_(std::move(itemConverters))
    { }

    virtual void Consume(TYsonPullParserCursor* cursor) override
    {
        YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::BeginList);
        cursor->Next();

        for (const auto& itemConverter : ItemConverters_) {
            itemConverter->Consume(cursor);
        }

        YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::EndList);
        cursor->Next();
    }

    virtual void ConsumeNull() override
    {
        // Null is represented as a tuple of defaults.
        for (const auto& itemConverter : ItemConverters_) {
            itemConverter->ConsumeNull();
        }
    }

    virtual DB::ColumnPtr FlushColumn() override
    {
        std::vector<DB::IColumn::MutablePtr> underlyingColumns;

        for (const auto& itemConverter : ItemConverters_) {
            underlyingColumns.emplace_back(itemConverter->FlushColumn()->assumeMutable());
        }
        return DB::ColumnTuple::create(std::move(underlyingColumns));
    }

    virtual DB::DataTypePtr GetDataType() const override
    {
        std::vector<DB::DataTypePtr> dataTypes;
        for (const auto& itemConverter : ItemConverters_) {
            dataTypes.emplace_back(itemConverter->GetDataType());
        }
        return std::make_shared<DB::DataTypeTuple>(dataTypes);
    }

private:
    std::vector<IConverterPtr> ItemConverters_;
};

////////////////////////////////////////////////////////////////////////////////

class TStructConverter
    : public IConverter
{
public:
    TStructConverter(std::vector<IConverterPtr> fieldConverters, std::vector<TString> fieldNames)
        : FieldConverters_(std::move(fieldConverters))
        , FieldNames_(std::move(fieldNames))
    {
        for (const auto& [index, fieldName] : Enumerate(FieldNames_)) {
            FieldNameToPosition_[fieldName] = index;
        }
    }

    virtual void Consume(TYsonPullParserCursor* cursor) override
    {
        if (cursor->GetCurrent().GetType() == EYsonItemType::BeginList) {
            ConsumePositional(cursor);
        } else if (cursor->GetCurrent().GetType() == EYsonItemType::BeginMap) {
            ConsumeNamed(cursor);
        } else {
            YT_ABORT();
        }
    }

    virtual void ConsumeNull() override
    {
        // Null is represented as a tuple of defaults.
        for (const auto& fieldConverter : FieldConverters_) {
            fieldConverter->ConsumeNull();
        }
    }

    virtual DB::ColumnPtr FlushColumn() override
    {
        std::vector<DB::IColumn::MutablePtr> underlyingColumns;

        for (const auto& fieldConverter : FieldConverters_) {
            underlyingColumns.emplace_back(fieldConverter->FlushColumn()->assumeMutable());
        }
        return DB::ColumnTuple::create(std::move(underlyingColumns));
    }

    virtual DB::DataTypePtr GetDataType() const override
    {
        std::vector<DB::DataTypePtr> dataTypes;
        for (const auto& FieldConverter : FieldConverters_) {
            dataTypes.emplace_back(FieldConverter->GetDataType());
        }
        return std::make_shared<DB::DataTypeTuple>(dataTypes, std::vector<std::string>(FieldNames_.begin(), FieldNames_.end()));
    }

private:
    std::vector<IConverterPtr> FieldConverters_;
    std::vector<TString> FieldNames_;
    THashMap<TString, int> FieldNameToPosition_;

    void ConsumeNamed(TYsonPullParserCursor* cursor)
    {
        YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::BeginMap);
        cursor->Next();
        std::vector<bool> seenPositions(FieldConverters_.size());
        while (cursor->GetCurrent().GetType() != EYsonItemType::EndMap) {
            auto key = cursor->GetCurrent().UncheckedAsString();
            auto position = GetOrCrash(FieldNameToPosition_, key);
            cursor->Next();
            YT_VERIFY(!seenPositions[position]);
            seenPositions[position] = true;
            FieldConverters_[position]->Consume(cursor);
        }
        YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::EndMap);
        cursor->Next();

        for (int index = 0; index < seenPositions.size(); ++index) {
            if (!seenPositions[index]) {
                FieldConverters_[index]->ConsumeNull();
            }
        }
    }

    void ConsumePositional(TYsonPullParserCursor* cursor)
    {
        YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::BeginList);
        cursor->Next();
        for (int index = 0; index < FieldConverters_.size(); ++index) {
            if (cursor->GetCurrent().GetType() == EYsonItemType::EndList) {
                FieldConverters_[index]->ConsumeNull();
            } else {
                FieldConverters_[index]->Consume(cursor);
            }
        }
        YT_VERIFY(cursor->GetCurrent().GetType() == EYsonItemType::EndList);
        cursor->Next();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeValueToClickHouseColumnConverter::TImpl
{
public:
    TImpl(TComplexTypeFieldDescriptor descriptor, TCompositeSettingsPtr settings)
        : Descriptor_(std::move(descriptor))
        , Settings_(std::move(settings))
    {
        RootConverter_ = CreateConverter(Descriptor_);
    }

    void ConsumeYson(TStringBuf yson)
    {
        TMemoryInput in(yson);
        TYsonPullParser parser(&in, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);
        RootConverter_->Consume(&cursor);

        YT_VERIFY(cursor->IsEndOfStream());
    }

    void ConsumeNull()
    {
        // This may result in either adding null or default value in case top-most type is not
        // enclosible in Nullable.
        RootConverter_->ConsumeNull();
    }

    DB::ColumnPtr FlushColumn()
    {
        return RootConverter_->FlushColumn();
    }

    DB::DataTypePtr GetDataType() const
    {
        return RootConverter_->GetDataType();
    }

private:
    TComplexTypeFieldDescriptor Descriptor_;
    TCompositeSettingsPtr Settings_;

    IConverterPtr RootConverter_;

    IConverterPtr CreateSimpleLogicalTypeConverter(ESimpleLogicalValueType valueType, const TComplexTypeFieldDescriptor& descriptor)
    {
        IConverterPtr converter;
        switch (valueType) {
            #define CASE(caseValueType, TCppType, TColumn, dataType)                                       \
                case caseValueType:                                                                        \
                    converter = std::make_unique<TSimpleValueConverter<caseValueType, TCppType, TColumn>>( \
                        descriptor,                                                                        \
                        dataType);                                                                         \
                    break;

            #define CASE_NUMERIC(caseValueType, TCppType) CASE(caseValueType, TCppType, DB::ColumnVector<TCppType>, std::make_shared<DB::DataTypeNumber<TCppType>>())

            CASE_NUMERIC(ESimpleLogicalValueType::Boolean, DB::UInt8)
            CASE_NUMERIC(ESimpleLogicalValueType::Uint8, DB::UInt8)
            CASE_NUMERIC(ESimpleLogicalValueType::Uint16, ui16)
            CASE_NUMERIC(ESimpleLogicalValueType::Uint32, ui32)
            CASE_NUMERIC(ESimpleLogicalValueType::Uint64, ui64)
            CASE_NUMERIC(ESimpleLogicalValueType::Int8, i8)
            CASE_NUMERIC(ESimpleLogicalValueType::Int16, i16)
            CASE_NUMERIC(ESimpleLogicalValueType::Int32, i32)
            CASE_NUMERIC(ESimpleLogicalValueType::Int64, i64)
            CASE_NUMERIC(ESimpleLogicalValueType::Float, float)
            CASE_NUMERIC(ESimpleLogicalValueType::Double, double)
            CASE_NUMERIC(ESimpleLogicalValueType::Interval, i64)
            CASE_NUMERIC(ESimpleLogicalValueType::Timestamp, ui64)
            // TODO(max42): specify timezone explicitly here.
            CASE(ESimpleLogicalValueType::Date, ui16, DB::ColumnVector<ui16>, std::make_shared<DB::DataTypeDate>())
            CASE(ESimpleLogicalValueType::Datetime, ui32, DB::ColumnVector<ui32>, std::make_shared<DB::DataTypeDateTime>())
            CASE(ESimpleLogicalValueType::String, DB::UInt8 /* actually unused */, DB::ColumnString, std::make_shared<DB::DataTypeString>())
            CASE(ESimpleLogicalValueType::Utf8, DB::UInt8 /* actually unused */, DB::ColumnString, std::make_shared<DB::DataTypeString>())
            CASE(ESimpleLogicalValueType::Void, DB::UInt8 /* actually unused */, DB::ColumnNothing, std::make_shared<DB::DataTypeNothing>())
            default:
                ThrowConversionError(descriptor, "simple logical value type %v is not supported", valueType);
        }
        return converter;
    }

    IConverterPtr CreateOptionalConverter(const TComplexTypeFieldDescriptor& descriptor)
    {
        // Descend to first non-optional enclosed type.
        auto nonOptionalDescriptor = descriptor;
        int nestingLevel = 0;
        while (nonOptionalDescriptor.GetType()->IsNullable()) {
            nonOptionalDescriptor = nonOptionalDescriptor.OptionalElement();
            ++nestingLevel;
        }

        YT_VERIFY(nestingLevel > 0);

        auto underlyingConverter = CreateConverter(nonOptionalDescriptor);

        return std::make_unique<TOptionalConverter>(std::move(underlyingConverter), nestingLevel);
    }

    IConverterPtr CreateListConverter(const TComplexTypeFieldDescriptor& descriptor)
    {
        auto underlyingConverter = CreateConverter(descriptor.ListElement());

        return std::make_unique<TListConverter>(std::move(underlyingConverter));
    }

    IConverterPtr CreateDictConverter(const TComplexTypeFieldDescriptor& descriptor)
    {
        auto keyConverter = CreateConverter(descriptor.DictKey());
        auto valueConverter = CreateConverter(descriptor.DictValue());

        return std::make_unique<TDictConverter>(std::move(keyConverter), std::move(valueConverter));
    }

    IConverterPtr CreateTupleConverter(const TComplexTypeFieldDescriptor& descriptor)
    {
        auto tupleLength = descriptor.GetType()->AsTupleTypeRef().GetElements().size();
        std::vector<IConverterPtr> itemConverters;
        for (int index = 0; index < tupleLength; ++index) {
            itemConverters.emplace_back(CreateConverter(descriptor.TupleElement(index)));
        }

        return std::make_unique<TTupleConverter>(std::move(itemConverters));
    }

    IConverterPtr CreateStructConverter(const TComplexTypeFieldDescriptor& descriptor)
    {
        auto structLength = descriptor.GetType()->AsStructTypeRef().GetFields().size();
        std::vector<IConverterPtr> fieldConverters;
        std::vector<TString> fieldNames;
        for (const auto& structField : descriptor.GetType()->AsStructTypeRef().GetFields()) {
            fieldNames.emplace_back(structField.Name);
        }
        for (int index = 0; index < structLength; ++index) {
            fieldConverters.emplace_back(CreateConverter(descriptor.StructField(index)));
        }

        return std::make_unique<TStructConverter>(std::move(fieldConverters), std::move(fieldNames));
    }

    IConverterPtr CreateConverter(const TComplexTypeFieldDescriptor& descriptor)
    {
        const auto& type = descriptor.GetType();
        if (type->GetMetatype() == ELogicalMetatype::Simple) {
            const auto& simpleType = type->AsSimpleTypeRef();
            if (simpleType.GetElement() == ESimpleLogicalValueType::Any ||
                simpleType.GetElement() == ESimpleLogicalValueType::Null ||
                simpleType.GetElement() == ESimpleLogicalValueType::Void)
            {
                return std::make_unique<TRawYsonToStringConverter>(descriptor, Settings_);
            } else {
                return CreateSimpleLogicalTypeConverter(simpleType.GetElement(), descriptor);
            }
        } else if (type->GetMetatype() == ELogicalMetatype::Optional) {
            return CreateOptionalConverter(descriptor);
        } else if (type->GetMetatype() == ELogicalMetatype::List) {
            return CreateListConverter(descriptor);
        } else if (type->GetMetatype() == ELogicalMetatype::Dict) {
            return CreateDictConverter(descriptor);
        } else if (type->GetMetatype() == ELogicalMetatype::Tuple) {
            return CreateTupleConverter(descriptor);
        } else if (type->GetMetatype() == ELogicalMetatype::Struct) {
            return CreateStructConverter(descriptor);
        } else {
            // Perform fallback to raw yson.
            return std::make_unique<TRawYsonToStringConverter>(descriptor, Settings_);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TCompositeValueToClickHouseColumnConverter::TCompositeValueToClickHouseColumnConverter(
    TComplexTypeFieldDescriptor descriptor,
    TCompositeSettingsPtr settings)
    : Impl_(std::make_unique<TImpl>(std::move(descriptor), std::move(settings)))
{ }

void TCompositeValueToClickHouseColumnConverter::ConsumeYson(TStringBuf yson)
{
    return Impl_->ConsumeYson(yson);
}

DB::ColumnPtr TCompositeValueToClickHouseColumnConverter::FlushColumn()
{
    return Impl_->FlushColumn();
}

DB::DataTypePtr TCompositeValueToClickHouseColumnConverter::GetDataType() const
{
    return Impl_->GetDataType();
}

void TCompositeValueToClickHouseColumnConverter::ConsumeNull()
{
    return Impl_->ConsumeNull();
}

TCompositeValueToClickHouseColumnConverter::~TCompositeValueToClickHouseColumnConverter() = default;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
