#include "named_structures_yson.h"

#include "scanner_factory.h"

#include <yt/client/table_client/logical_type.h>

#include <util/generic/buffer.h>
#include <util/stream/buffer.h>

namespace NYT::NComplexTypes {

using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TTypeTrivialityCache
{
public:
    explicit TTypeTrivialityCache(const TLogicalTypePtr& logicalType)
    {
        CheckAndCacheTriviality(logicalType);
    }

    bool IsTrivial(const TLogicalTypePtr& logicalType) const
    {
        return GetOrCrash(Cache_, logicalType.Get());
    }

private:
    bool CheckAndCacheTriviality(const TLogicalTypePtr& logicalType)
    {
        auto& result = Cache_[logicalType.Get()];
        switch (logicalType->GetMetatype()) {
            case ELogicalMetatype::Simple:
                return result = true;

            case ELogicalMetatype::Optional:
            case ELogicalMetatype::List:
            case ELogicalMetatype::Tagged:
                return result = CheckAndCacheTriviality(logicalType->GetElement());

            case ELogicalMetatype::Tuple:
            case ELogicalMetatype::VariantTuple: {
                result = true;
                for (const auto& element : logicalType->GetElements()) {
                    if (!CheckAndCacheTriviality(element)) {
                        result = false;
                        // no break here, we want to cache all elements
                    }
                }
                return result;
            }

            case ELogicalMetatype::Struct:
            case ELogicalMetatype::VariantStruct: {
                for (const auto& field : logicalType->GetFields()) {
                    CheckAndCacheTriviality(field.Type);
                }
                return result = false;
            }

            case ELogicalMetatype::Dict:
                result = true;
                if (!CheckAndCacheTriviality(logicalType->AsDictTypeRef().GetKey())) {
                    result = false;
                }
                if (!CheckAndCacheTriviality(logicalType->AsDictTypeRef().GetValue())) {
                    result = false;
                }
                return result;
        }
        YT_ABORT();
    }

private:
    THashMap<void*, bool> Cache_;
};

////////////////////////////////////////////////////////////////////////////////

void IdRecoder(TYsonPullParserCursor* cursor, NYson::IYsonConsumer* consumer)
{
    cursor->TransferComplexValue(consumer);
}

////////////////////////////////////////////////////////////////////////////////

struct TStructFieldInfo
{
    TYsonConverter Converter;
    TString FieldName;
    bool IsNullable = false;
};


template<bool IsElementNullable>
class TOptionalHandler
{
public:
    void OnEmptyOptional(IYsonConsumer* consumer) const
    {
        consumer->OnEntity();
    }

    void OnFilledOptional(const TYsonConverter& recoder, TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        if constexpr (IsElementNullable) {
            consumer->OnBeginList();
            consumer->OnListItem();
            recoder(cursor, consumer);
            consumer->OnEndList();
        } else {
            recoder(cursor, consumer);
        }
    }
};

class TListHandler
{
public:
    Y_FORCE_INLINE void OnListBegin(IYsonConsumer* consumer) const
    {
        consumer->OnBeginList();
    }

    Y_FORCE_INLINE void OnListItem(
        const TYsonConverter& recoder,
        TYsonPullParserCursor* cursor,
        IYsonConsumer* consumer) const
    {
        consumer->OnListItem();
        recoder(cursor, consumer);
    }

    Y_FORCE_INLINE void OnListEnd(IYsonConsumer* consumer) const
    {
        consumer->OnEndList();
    }
};

class TTupleApplier
{
public:
    Y_FORCE_INLINE void OnTupleBegin(IYsonConsumer* consumer) const
    {
        consumer->OnBeginList();
    }

    Y_FORCE_INLINE void
    OnTupleItem(const TYsonConverter& recoder, TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        consumer->OnListItem();
        recoder(cursor, consumer);
    }

    Y_FORCE_INLINE void OnTupleEnd(IYsonConsumer* consumer) const
    {
        consumer->OnEndList();
    }
};

template <bool SkipNullValues>
class TStructApplier
{
public:
    Y_FORCE_INLINE void OnStructBegin(IYsonConsumer* consumer) const
    {
        consumer->OnBeginMap();
    }

    Y_FORCE_INLINE void OnStructEnd(IYsonConsumer* consumer) const
    {
        consumer->OnEndMap();
    }

    Y_FORCE_INLINE void OnStructField(
        const TStructFieldInfo& field,
        TYsonPullParserCursor* cursor,
        IYsonConsumer* consumer) const
    {
        if constexpr (SkipNullValues) {
            if (field.IsNullable && (*cursor)->GetType() == EYsonItemType::EntityValue) {
                cursor->Next();
                return;
            }
        }
        consumer->OnKeyedItem(field.FieldName);
        field.Converter(cursor, consumer);
    }
};

class TVariantTupleApplier
{
public:
    Y_FORCE_INLINE void OnVariantAlternative(
        const std::pair<int, TYsonConverter>& alternative,
        TYsonPullParserCursor* cursor,
        IYsonConsumer* consumer) const
    {
        consumer->OnBeginList();

        consumer->OnListItem();
        consumer->OnInt64Scalar(alternative.first);

        consumer->OnListItem();
        alternative.second(cursor, consumer);

        consumer->OnEndList();
    }
};

class TVariantStructApplier
{
public:
    Y_FORCE_INLINE void OnVariantAlternative(
        const std::pair<TString, TYsonConverter>& alternative,
        TYsonPullParserCursor* cursor,
        IYsonConsumer* consumer) const
    {
        consumer->OnBeginList();

        consumer->OnListItem();
        consumer->OnStringScalar(alternative.first);

        consumer->OnListItem();
        alternative.second(cursor, consumer);

        consumer->OnEndList();
    }
};

class TDictApplier
{
public:
    Y_FORCE_INLINE void OnDictBegin(IYsonConsumer* consumer) const
    {
        consumer->OnBeginList();
    }

    Y_FORCE_INLINE void
    OnKey(const TYsonConverter& keyRecoder, TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        consumer->OnListItem();
        consumer->OnBeginList();
        consumer->OnListItem();
        keyRecoder(cursor, consumer);
    }

    Y_FORCE_INLINE void
    OnValue(const TYsonConverter& valueRecoder, TYsonPullParserCursor* cursor, IYsonConsumer* consumer) const
    {
        consumer->OnListItem();
        valueRecoder(cursor, consumer);
        consumer->OnEndList();
    }

    Y_FORCE_INLINE void OnDictEnd(IYsonConsumer* consumer) const
    {
        consumer->OnEndList();
    }
};

class TNamedToPositionalStructConverter
{
public:
    TNamedToPositionalStructConverter(TComplexTypeFieldDescriptor descriptor, const std::vector<TStructFieldInfo>& fields)
        : BufferOutput_(Buffer_)
        , YsonWriter_(&BufferOutput_, EYsonType::ListFragment)
        , Descriptor_(std::move(descriptor))
    {
        PositionTable_.reserve(fields.size());
        for (size_t i = 0; i < fields.size(); ++i) {
            FieldMap_[fields[i].FieldName] = {fields[i].Converter, static_cast<int>(i)};

            PositionTable_.emplace_back();
            PositionTable_.back().IsNullable = fields[i].IsNullable;
            PositionTable_.back().FieldName = fields[i].FieldName;
        }
    }

    // NB. to wrap this object into std::function we must be able to copy it.
    TNamedToPositionalStructConverter(const TNamedToPositionalStructConverter& other)
        : FieldMap_(other.FieldMap_)
        , PositionTable_(other.PositionTable_)
        , Buffer_(other.Buffer_)
        , BufferOutput_(Buffer_)
        , YsonWriter_(&BufferOutput_, EYsonType::ListFragment)
        , Descriptor_(other.Descriptor_)
        , CurrentGeneration_(other.CurrentGeneration_)
    { }

    void operator () (TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
    {
        IncrementGeneration();

        EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::BeginMap);
        cursor->Next();
        Buffer_.Clear();

        YT_ASSERT(YsonWriter_.GetDepth() == 0);

        while ((*cursor)->GetType() != EYsonItemType::EndMap) {
            EnsureYsonToken(Descriptor_, *cursor, EYsonItemType::StringValue);
            auto fieldName = (*cursor)->UncheckedAsString();
            auto it = FieldMap_.find(fieldName);
            if (it == FieldMap_.end()) {
                THROW_ERROR_EXCEPTION(
                    "Unknown field %Qv while parsing %Qv",
                    fieldName,
                    Descriptor_.GetDescription());
            }
            cursor->Next();

            auto& positionEntry = PositionTable_[it->second.Position];
            if (positionEntry.Generation == CurrentGeneration_) {
                THROW_ERROR_EXCEPTION(
                    "Multiple occurrences of field %Qv while parsing %Qv",
                    it->first, // NB. it's not safe to use fieldName since we moved cursor
                    Descriptor_.GetDescription());
            }

            auto offset = Buffer_.Size();
            it->second.Converter(cursor, &YsonWriter_);
            YsonWriter_.Flush();

            positionEntry.Offset = offset;
            positionEntry.Size = Buffer_.size() - offset;
            positionEntry.Generation = CurrentGeneration_;
        }

        // Skip map end token.
        cursor->Next();

        consumer->OnBeginList();
        for (const auto& positionEntry : PositionTable_) {
            if (positionEntry.Generation == CurrentGeneration_) {
                auto yson = TStringBuf(Buffer_.Data() + positionEntry.Offset, positionEntry.Size);
                consumer->OnRaw(yson, EYsonType::ListFragment);
            } else if (positionEntry.IsNullable) {
                consumer->OnRaw("#;", EYsonType::ListFragment);
            } else {
                THROW_ERROR_EXCEPTION("Field %Qv is missing while parsing %Qv",
                    positionEntry.FieldName,
                    Descriptor_.GetDescription());
            }
        }
        consumer->OnEndList();
    }
private:
    void IncrementGeneration()
    {
        if (++CurrentGeneration_ == 0) {
            for (auto& entry : PositionTable_) {
                entry.Generation = 0;
            }
            CurrentGeneration_ = 1;
        }
    }

private:
    struct TFieldMapEntry
    {
        TYsonConverter Converter;
        int Position = 0;
    };

    struct TPositionTableEntry {
        size_t Offset = 0;
        size_t Size = 0;
        ui16 Generation = 0;
        bool IsNullable = false;
        TStringBuf FieldName;
    };

    THashMap<TString, TFieldMapEntry> FieldMap_;
    std::vector<TPositionTableEntry> PositionTable_;
    TBuffer Buffer_;
    TBufferOutput BufferOutput_;
    TBufferedBinaryYsonWriter YsonWriter_;
    TComplexTypeFieldDescriptor Descriptor_;
    ui16 CurrentGeneration_;
};

TYsonConverter CreateNamedToPositionalVariantStructConverter(
    TComplexTypeFieldDescriptor descriptor,
    std::vector<std::pair<TString, TYsonConverter>> fieldConverters)
{
    THashMap<TString, std::pair<int, TYsonConverter>> typeMap;
    int fieldIndex = 0;
    for (const auto& [fieldName, converter] : fieldConverters) {
        typeMap[fieldName] = {fieldIndex, converter};
        ++fieldIndex;
    }

    return [
        descriptor=descriptor,
        typeMap=std::move(typeMap)
    ] (TYsonPullParserCursor* cursor, IYsonConsumer* ysonConsumer) {
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::BeginList);
        cursor->Next();
        EnsureYsonToken(descriptor, *cursor, EYsonItemType::StringValue);
        auto fieldName = (*cursor)->UncheckedAsString();
        auto it = typeMap.find(fieldName);
        if (it == typeMap.end()) {
            THROW_ERROR_EXCEPTION(
                "Unknown variant field %Qv while parsing %Qv",
                fieldName,
                descriptor.GetDescription());
        }
        cursor->Next();

        const auto& [variantIndex, converter] = it->second;
        ysonConsumer->OnBeginList();

        ysonConsumer->OnListItem();
        ysonConsumer->OnInt64Scalar(variantIndex);

        ysonConsumer->OnListItem();
        converter(cursor, ysonConsumer);

        ysonConsumer->OnEndList();

        EnsureYsonToken(descriptor, *cursor, EYsonItemType::EndList);
        cursor->Next();
    };
}


using TYsonConsumerScannerFactory = TScannerFactory<IYsonConsumer*>;

struct TNamedToPositionalConfig
{ };

using TConfigVariant = std::variant<TPositionalToNamedConfig, TNamedToPositionalConfig>;

TYsonConverter CreateYsonConverterImpl(
    const TComplexTypeFieldDescriptor& descriptor,
    const TTypeTrivialityCache& cache,
    const TConfigVariant& config)
{
    const auto& type = descriptor.GetType();
    if (cache.IsTrivial(type)) {
        return IdRecoder;
    }
    switch (type->GetMetatype()) {
        case ELogicalMetatype::Simple:
            // All simple types are trivial and must be handled before this switch
            YT_ABORT();
        case ELogicalMetatype::Optional: {
            auto elementConverter = CreateYsonConverterImpl(descriptor.OptionalElement(), cache, config);
            if (type->AsOptionalTypeRef().IsElementNullable()) {
                return TYsonConsumerScannerFactory::CreateOptionalScanner(
                    descriptor,
                    TOptionalHandler<true>(),
                    elementConverter);
            } else {
                return TYsonConsumerScannerFactory::CreateOptionalScanner(
                    descriptor,
                    TOptionalHandler<false>(),
                    elementConverter);
            };
        }
        case ELogicalMetatype::List: {
            auto elementConverter = CreateYsonConverterImpl(descriptor.ListElement(), cache, config);
            return TYsonConsumerScannerFactory::CreateListScanner(descriptor, TListHandler(), elementConverter);
        }
        case ELogicalMetatype::Tuple: {
            std::vector<TYsonConverter> elementConverters;
            const auto size = type->GetElements().size();
            for (size_t i = 0; i != size; ++i) {
                elementConverters.push_back(CreateYsonConverterImpl(descriptor.TupleElement(i), cache, config));
            }
            return TYsonConsumerScannerFactory::CreateTupleScanner(descriptor, TTupleApplier(), elementConverters);
        }
        case ELogicalMetatype::Struct: {
            const auto& fields = type->GetFields();
            std::vector<TStructFieldInfo> fieldInfos;
            for (size_t i = 0; i != fields.size(); ++i) {
                fieldInfos.emplace_back();
                fieldInfos.back().FieldName = fields[i].Name;
                fieldInfos.back().Converter = CreateYsonConverterImpl(descriptor.StructField(i), cache, config);
                fieldInfos.back().IsNullable = fields[i].Type->IsNullable();
            }
            if (std::holds_alternative<TNamedToPositionalConfig>(config)) {
                return TNamedToPositionalStructConverter(descriptor, fieldInfos);
            } else {
                YT_ASSERT(std::holds_alternative<TPositionalToNamedConfig>(config));
                auto skipNullValues = std::get<TPositionalToNamedConfig>(config).SkipNullValues;
                if (skipNullValues) {
                    return TYsonConsumerScannerFactory::CreateStructScanner(
                        descriptor, TStructApplier<true>(), fieldInfos);
                } else {
                    return TYsonConsumerScannerFactory::CreateStructScanner(
                        descriptor, TStructApplier<false>(), fieldInfos);
                }
            }
        }
        case ELogicalMetatype::VariantTuple: {
            std::vector<std::pair<int,TYsonConverter>> elementConverters;
            const auto size = type->GetElements().size();
            for (size_t i = 0; i != size; ++i) {
                elementConverters.emplace_back(i, CreateYsonConverterImpl(descriptor.VariantTupleElement(i), cache, config));
            }
            return TYsonConsumerScannerFactory::CreateVariantScanner(descriptor, TVariantTupleApplier(), elementConverters);
        }
        case ELogicalMetatype::VariantStruct: {
            std::vector<std::pair<TString, TYsonConverter>> elementConverters;
            const auto& fields = type->GetFields();
            for (size_t i = 0; i != fields.size(); ++i) {
                elementConverters.emplace_back(
                    fields[i].Name,
                    CreateYsonConverterImpl(descriptor.VariantStructField(i), cache, config));
            }
            if (std::holds_alternative<TNamedToPositionalConfig>(config)) {
                return CreateNamedToPositionalVariantStructConverter(descriptor, elementConverters);
            } else {
                YT_ASSERT(std::holds_alternative<TPositionalToNamedConfig>(config));
                return TYsonConsumerScannerFactory::CreateVariantScanner(
                    descriptor, TVariantStructApplier(), elementConverters);
            }
        }
        case ELogicalMetatype::Dict: {
            auto keyConverter = CreateYsonConverterImpl(descriptor.DictKey(), cache, config);
            auto valueConverter = CreateYsonConverterImpl(descriptor.DictValue(), cache, config);
            return TYsonConsumerScannerFactory::CreateDictScanner(
                descriptor,
                TDictApplier(),
                keyConverter,
                valueConverter);
        }
        case ELogicalMetatype::Tagged:
            return CreateYsonConverterImpl(descriptor.TaggedElement(), cache, config);
    }
    YT_ABORT();
}

TYsonConverter CreatePositionalToNamedYsonConverter(
    const TComplexTypeFieldDescriptor& descriptor,
    const TPositionalToNamedConfig& config)
{
    TTypeTrivialityCache cache(descriptor.GetType());
    return CreateYsonConverterImpl(descriptor, cache, config);
}

////////////////////////////////////////////////////////////////////////////////

TYsonConverter CreateNamedToPositionalYsonConverter(const TComplexTypeFieldDescriptor& descriptor)
{
    TTypeTrivialityCache cache(descriptor.GetType());
    return CreateYsonConverterImpl(descriptor, cache, TNamedToPositionalConfig());
}

////////////////////////////////////////////////////////////////////////////////

void ApplyYsonConverter(const TYsonConverter& converter, TStringBuf inputYson, NYson::IYsonConsumer* consumer)
{
    TMemoryInput in(inputYson);
    TYsonPullParser parser(&in, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);
    converter(&cursor, consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
