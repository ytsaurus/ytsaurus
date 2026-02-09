#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
// block_builder.h includes mkql_type_builder.h which in turnincludes defs.h where we have defined THROW macro.
#undef THROW

#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/public/udf/arrow/args_dechunker.h>
#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_type_printer.h>
#include <yql/essentials/utils/utf8.h>

#include <util/str_stl.h>

#include <contrib/clickhouse/base/base/JSON.h>

#include <contrib/clickhouse/src/AggregateFunctions/registerAggregateFunctions.h>
#include <contrib/clickhouse/src/Client/Connection.h>
#include <contrib/clickhouse/src/Client/ConnectionPool.h>
#include <contrib/clickhouse/src/Client/ConnectionPoolWithFailover.h>
#include <contrib/clickhouse/src/Columns/ColumnAggregateFunction.h>
#include <contrib/clickhouse/src/Columns/ColumnArray.h>
#include <contrib/clickhouse/src/Columns/ColumnConst.h>
#include <contrib/clickhouse/src/Columns/ColumnNullable.h>
#include <contrib/clickhouse/src/Columns/ColumnTuple.h>
#include <contrib/clickhouse/src/Columns/ColumnsNumber.h>
#include <contrib/clickhouse/src/Columns/ColumnString.h>
#include <contrib/clickhouse/src/Databases/DatabaseMemory.h>
#include <contrib/clickhouse/src/QueryPipeline/RemoteQueryExecutor.h>
#include <contrib/clickhouse/src/DataTypes/DataTypeAggregateFunction.h>
#include <contrib/clickhouse/src/DataTypes/DataTypeArray.h>
#include <contrib/clickhouse/src/DataTypes/DataTypeDate.h>
#include <contrib/clickhouse/src/DataTypes/DataTypeDateTime.h>
#include <contrib/clickhouse/src/DataTypes/DataTypeEnum.h>
#include <contrib/clickhouse/src/DataTypes/DataTypeFactory.h>
#include <contrib/clickhouse/src/DataTypes/DataTypesNumber.h>
#include <contrib/clickhouse/src/DataTypes/DataTypeString.h>
#include <contrib/clickhouse/src/DataTypes/DataTypeUUID.h>
#include <contrib/clickhouse/src/DataTypes/DataTypeNothing.h>
#include <contrib/clickhouse/src/DataTypes/DataTypeNullable.h>
#include <contrib/clickhouse/src/DataTypes/DataTypeTuple.h>
#include <contrib/clickhouse/src/Formats/FormatFactory.h>
#include <contrib/clickhouse/src/Formats/registerFormats.h>
#include <contrib/clickhouse/src/Functions/registerFunctions.h>
#include <contrib/clickhouse/src/Functions/FunctionFactory.h>
#include <contrib/clickhouse/src/Functions/FunctionHelpers.h>
#include <contrib/clickhouse/src/Interpreters/Context.h>
#include <contrib/clickhouse/src/Interpreters/executeQuery.h>
#include <contrib/clickhouse/src/IO/UseSSL.h>
#include <contrib/clickhouse/src/Processors/ISource.h>
#include <contrib/clickhouse/src/Storages/System/attachSystemTables.h>
#include <contrib/clickhouse/src/Storages/IStorage.h>
#include <contrib/clickhouse/src/TableFunctions/registerTableFunctions.h>
#include <contrib/clickhouse/src/Common/quoteString.h>
#include <contrib/clickhouse/src/Processors/Executors/PollingQueue.h>
#include <contrib/clickhouse/src/QueryPipeline/RemoteQueryExecutorReadContext.h>
#include <contrib/clickhouse/src/Processors/Executors/PullingPipelineExecutor.h>
#include <contrib/clickhouse/src/AggregateFunctions/IAggregateFunction.h>
#include <contrib/clickhouse/src/Core/Settings.h>
#include <contrib/clickhouse/src/Interpreters/DatabaseCatalog.h>
#include <contrib/clickhouse/src/Common/Config/ConfigProcessor.h>
#include <contrib/clickhouse/src/Interpreters/registerInterpreters.h>

#include <arrow/datum.h>
#include <arrow/type.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/util.h>
#include <arrow/c/bridge.h>

#include <DBPoco/Util/Application.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/stream/file.h>
#include <util/datetime/cputimer.h>
#include <util/system/env.h>
#include <util/string/split.h>
#include <util/folder/tempdir.h>
#include <util/folder/dirut.h>

using namespace NYql;
using namespace NUdf;

namespace {

static constexpr char CLICKHOUSE_GEODATA_ROOT[] = "YQL_UDF_CLICKHOUSE_GEODATA_ROOT";

TMaybe<TString> GetClickHouseGeoDataRoot() {
    static TMaybe<TString> Path = [](){
        return TryGetEnv(TString(CLICKHOUSE_GEODATA_ROOT));
    }();
    return Path;
}

} // namespace

namespace DB::Setting {

extern const SettingsUInt64 preferred_block_size_bytes;

} // namespace DB::Setting

namespace {

class INotify {
public:
    virtual ~INotify() = default;
    virtual void Do() = 0;
};

class TApp : public DBPoco::Util::Application {
};

class TConfigBase : public DBPoco::Util::AbstractConfiguration
{
public:
    void setRaw(const std::string& key, const std::string& value) override {
        Y_UNUSED(key);
        Y_UNUSED(value);
        throw yexception() << "Config cannot be modified";
    }

    void enumerate(const std::string& path, Keys& range) const override {
        Y_UNUSED(path);
        range = {};
    }
};

class TAppConfig : public TConfigBase {
public:
    bool getRaw(const std::string& key, std::string& value) const override {
        if (key == "openSSL.client.invalidCertificateHandler.name") {
            value = "RejectCertificateHandler";
            return true;
        }
        else if (key == "openSSL.client.verificationMode") {
            value = "none"; // TODO
            return true;
        }

        return false;
    }
};

class TServerConfig : public TConfigBase
{
public:
    bool getRaw(const std::string& key, std::string& value) const override {
        if (const std::unique_lock lock(Mutex_); key == "path_to_regions_hierarchy_file") {
            PrepareGeo();
            value = (GeoDataRoot_ / "regions_hierarchy.txt").GetPath();
            return true;
        }
        else if (key == "path_to_regions_names_files") {
            PrepareGeo();
            value = GeoDataRoot_.GetPath();
            return true;
        }

        return false;
    }

private:
    void PrepareGeo() const {
        if (!GeoDataRoot_) {
            if (const auto geoDataRoot = GetClickHouseGeoDataRoot()) {
                GeoDataRoot_ = *geoDataRoot;
                // GeoData is already extracted within the
                // directory, specified by env variable.
                IsGeoExtracted_ = true;
            } else {
                // XXX: Anchor temporary directory to prevent its
                // removal while using UDF.
                TmpDirAnchor_ = std::make_unique<TTempDir>();
                GeoDataRoot_ = TmpDirAnchor_->Path();
            }
        }

        if (!IsGeoExtracted_) {
            NResource::TResources resources;
            NResource::FindMatch("/geo/", &resources);
            for (const auto& r : resources) {
                auto file = r.Key;
                Y_ENSURE(file.SkipPrefix("/geo/"));
                TUnbufferedFileOutput out(GeoDataRoot_ / file);
                out.Write(r.Data.data(), r.Data.size());
                out.Finish();
            }

            IsGeoExtracted_ = true;
        }
    }

private:
    mutable std::mutex Mutex_;
    mutable TFsPath GeoDataRoot_;
    mutable std::unique_ptr<TTempDir> TmpDirAnchor_;
    mutable bool IsGeoExtracted_ = false;
};

struct THostContext : public DB::IHostContext {
public:
    THostContext(const TType* inputType, const ITypeInfoHelper::TPtr& typeHelper)
        : InputType(inputType)
        , TypeHelper(typeHelper)
    {
    }

    const TType* InputType = nullptr;
    const ITypeInfoHelper::TPtr TypeHelper;
    TUnboxedValue InputValue;
    const IValueBuilder* ValueBuilder = nullptr;
};

std::shared_ptr<THostContext> GetHostContext(DB::ContextPtr context) {
    return std::dynamic_pointer_cast<THostContext>(context->getHostContext());
}

struct TColumnMeta;

class IDBColumnReader {
public:
    using TPtr = std::shared_ptr<IDBColumnReader>;
    virtual ~IDBColumnReader() = default;
    virtual arrow::Datum Read(const DB::IColumn& input) = 0;
    static TPtr Make(const ITypeInfoHelper& typeInfoHelper, const TColumnMeta& meta, arrow::MemoryPool& pool);
};

class IDBColumnWriter {
public:
    using TPtr = std::shared_ptr<IDBColumnWriter>;
    virtual ~IDBColumnWriter() = default;
    void Reserve(DB::IColumn& output, size_t length) {
        output.reserve(length);
    }
    virtual size_t Write(DB::IColumn& output, const arrow::ArrayData& input) = 0;
    static TPtr Make(const TColumnMeta& meta);
};

struct TColumnMeta {
    std::optional<bool> IsScalarBlock; // empty if non-block mode
    std::shared_ptr<arrow::DataType> ArrowType;
    std::shared_ptr<IArrowType> HostArrowType;
    std::optional<size_t> MaxBlockLen;
    IDBColumnReader::TPtr Reader;
    IDBColumnWriter::TPtr Writer;

    std::optional<TString> Aggregation;
    std::optional<EDataSlot> Slot; // empty if null type
    bool IsOptional = false;
    bool IsEmptyList = false;
    bool IsList = false;
    bool IsTuple = false;
    ui8 Precision = 0, Scale = 0; // filled for Decimal
    std::vector<TColumnMeta> Items; // filled if IsTuple or IsList
    std::vector<TString> Enum;
    std::unordered_map<i16, std::pair<TString, ui32>> EnumVar;
};

void PermuteUuid(const char* src, char* dst, bool forward) {
    static ui32 Pairs[16] = { 4, 5, 6, 7, 2, 3, 0, 1, 15, 14, 13, 12, 11, 10, 9, 8 };
    static ui32 InvPairs[16] = { 6, 7, 4, 5, 0, 1, 2, 3, 15, 14, 13, 12, 11, 10, 9, 8 };
    for (ui32 i = 0; i < 16; ++i) {
        dst[forward ? Pairs[i] : InvPairs[i]] = src[i];
    }
}

template <typename TCH, typename TData>
class TFixedSizeDBColumnReader : public IDBColumnReader {
public:
    TFixedSizeDBColumnReader(const ITypeInfoHelper& typeInfoHelper, const TColumnMeta& meta, arrow::MemoryPool& pool)
        : ArrowType(meta.ArrowType)
        , Nullable(meta.IsOptional)
        , MaxBlockLen(*meta.MaxBlockLen)
        , Pool(pool)
    {
        Y_UNUSED(typeInfoHelper);
    }

    arrow::Datum Read(const DB::IColumn& input) final {
        const DB::IColumn* in = &input;
        const DB::NullMap* nullMapData = nullptr;
        if (Nullable) {
            auto nullable = static_cast<const DB::ColumnNullable*>(in);
            in = &nullable->getNestedColumn();
            nullMapData = &nullable->getNullMapData();
        }

        const auto& inData = dynamic_cast<const DB::ColumnVector<TCH>*>(in)->getData();
        const size_t length = inData.size();
        const TData* rawData = nullptr;
        if constexpr (std::is_same<TData, ui8>::value || std::is_same<TData, i8>::value) {
            // CH uses const char8_t for DB::UInt8 and _BitInt(8) for DB::Int8
            rawData = reinterpret_cast<const TData*>(inData.data());
        } else {
            rawData = inData.data();
        }
        TVector<std::shared_ptr<arrow::ArrayData>> outputArrays;
        for (size_t pos = 0; pos < length; ) {
            size_t chunkLen = std::min(length - pos, MaxBlockLen);

            TTypedBufferBuilder<TData> builder(&Pool);
            builder.Reserve(chunkLen);
            builder.UnsafeAppend(rawData + pos, chunkLen);
            auto dataBuffer = builder.Finish();

            std::shared_ptr<arrow::Buffer> chunkBitmap;
            if (Nullable) {
                chunkBitmap = MakeDenseBitmapNegate(reinterpret_cast<const ui8*>(nullMapData->data()) + pos, chunkLen, &Pool);
            }
            outputArrays.push_back(arrow::ArrayData::Make(ArrowType, chunkLen, { chunkBitmap, dataBuffer }));
            pos += chunkLen;
        }
        return MakeArray(outputArrays);
    }

private:
    const std::shared_ptr<arrow::DataType> ArrowType;
    const bool Nullable;
    const size_t MaxBlockLen;
    arrow::MemoryPool& Pool;
};

template <typename TStringType, bool Nullable>
class TStringDBColumnReader : public IDBColumnReader {
public:
    TStringDBColumnReader(const ITypeInfoHelper& typeInfoHelper, const TColumnMeta& meta, arrow::MemoryPool& pool)
        : Builder(typeInfoHelper, meta.ArrowType, pool, *meta.MaxBlockLen)
    {
    }

    arrow::Datum Read(const DB::IColumn& input) final {
        const DB::IColumn* in = &input;
        const DB::NullMap* nullMapData = nullptr;
        if constexpr (Nullable) {
            auto nullable = static_cast<const DB::ColumnNullable*>(in);
            in = &nullable->getNestedColumn();
            nullMapData = &nullable->getNullMapData();
        }

        const size_t length = in->size();
        const size_t maxLen = Builder.MaxLength();
        TVector<std::shared_ptr<arrow::ArrayData>> outputArrays;
        for (size_t i = 0; i < length; ) {
            size_t j = 0;
            for (; i < length && j < maxLen; ++i, ++j) {
                if constexpr (Nullable) {
                  if ((*nullMapData)[i]) {
                    Builder.Add(TBlockItem());
                    continue;
                  }
                }
                StringRef ref = in->getDataAt(i);
                Y_DEBUG_ABORT_UNLESS(ref.size <= std::numeric_limits<ui32>::max());
                Builder.Add(TBlockItem({ ref.data, static_cast<ui32>(ref.size) }));
            }
            bool finish = i == length;
            auto outputDatum = Builder.Build(finish);
            ForEachArrayData(outputDatum, [&](const auto& arr) {
                outputArrays.push_back(arr);
            });
        }
        return MakeArray(outputArrays);
    }

private:
    TStringArrayBuilder<TStringType, Nullable> Builder;
};

template <bool Nullable>
class TTupleDBColumnReader : public IDBColumnReader {
public:
    TTupleDBColumnReader(const ITypeInfoHelper& typeInfoHelper, const TColumnMeta& meta, arrow::MemoryPool& pool)
        : Pool(pool)
        , ArrowType(meta.ArrowType)
    {
        for (auto& child : meta.Items) {
            Children.push_back(IDBColumnReader::Make(typeInfoHelper, child, pool));
        }
    }

    arrow::Datum Read(const DB::IColumn& input) final {
        const DB::IColumn* in = &input;
        const DB::NullMap* nullMapData = nullptr;
        if constexpr (Nullable) {
            auto nullable = static_cast<const DB::ColumnNullable*>(in);
            in = &nullable->getNestedColumn();
            nullMapData = &nullable->getNullMapData();
        }

        const size_t length = in->size();
        const DB::ColumnTuple* inTuple = static_cast<const DB::ColumnTuple*>(in);
        std::vector<arrow::Datum> childDatums;
        for (size_t i = 0; i < Children.size(); ++i) {
            childDatums.push_back(Children[i]->Read(inTuple->getColumn(i)));
        }

        TVector<std::shared_ptr<arrow::ArrayData>> outputArrays;
        if (!Children.empty()) {
            TArgsDechunker dechunker(std::move(childDatums));
            size_t pos = 0;
            std::vector<arrow::Datum> chunk;
            while (dechunker.Next(chunk)) {
                std::vector<std::shared_ptr<arrow::ArrayData>> childArrays;
                for (auto& childDatum : chunk) {
                    Y_ENSURE(childDatum.is_array());
                    childArrays.push_back(childDatum.array());
                }
                size_t chunkLen = childArrays.front()->length;
                std::shared_ptr<arrow::Buffer> chunkBitmap;
                if constexpr (Nullable) {
                    chunkBitmap = MakeDenseBitmapNegate(reinterpret_cast<const ui8*>(nullMapData->data()) + pos, chunkLen, &Pool);
                }
                outputArrays.push_back(arrow::ArrayData::Make(ArrowType, chunkLen, { chunkBitmap }, std::move(childArrays)));
                pos += chunkLen;
            }
            Y_ENSURE(pos == length);
        } else {
            std::shared_ptr<arrow::Buffer> chunkBitmap;
            if constexpr (Nullable) {
                chunkBitmap = MakeDenseBitmapNegate(reinterpret_cast<const ui8*>(nullMapData->data()), length, &Pool);
            }
            outputArrays.push_back(arrow::ArrayData::Make(ArrowType, length, { chunkBitmap }, {}));
        }

        return MakeArray(outputArrays);
    }

private:
    TVector<IDBColumnReader::TPtr> Children;
    arrow::MemoryPool& Pool;
    const std::shared_ptr<arrow::DataType> ArrowType;
};

template<typename TCH, typename TData>
IDBColumnReader::TPtr MakeFixedSizeDBColumnReader(const ITypeInfoHelper& typeInfoHelper, const TColumnMeta& meta, arrow::MemoryPool& pool) {
    return std::make_shared<TFixedSizeDBColumnReader<TCH, TData>>(typeInfoHelper, meta, pool);
}

template<typename TStringType>
IDBColumnReader::TPtr MakeStringDBColumnReader(const ITypeInfoHelper& typeInfoHelper, const TColumnMeta& meta, arrow::MemoryPool& pool) {
    if (meta.IsOptional) {
        return std::make_shared<TStringDBColumnReader<TStringType, true>>(typeInfoHelper, meta, pool);
    }
    return std::make_shared<TStringDBColumnReader<TStringType, false>>(typeInfoHelper, meta, pool);
}

IDBColumnReader::TPtr IDBColumnReader::Make(const ITypeInfoHelper& typeInfoHelper, const TColumnMeta& meta, arrow::MemoryPool& pool) {
    Y_ENSURE(meta.ArrowType);

    if (meta.IsTuple) {
        if (meta.IsOptional) {
            return std::make_shared<TTupleDBColumnReader<true>>(typeInfoHelper, meta, pool);
        }
        return std::make_shared<TTupleDBColumnReader<false>>(typeInfoHelper, meta, pool);
    }

    Y_ENSURE(meta.Slot);
    IDBColumnReader::TPtr result;

    switch (*meta.Slot) {
    case EDataSlot::String:
    case EDataSlot::Yson:
        result = MakeStringDBColumnReader<arrow::BinaryType>(typeInfoHelper, meta, pool);
        break;
    case EDataSlot::Utf8:
    case EDataSlot::Json:
        result = MakeStringDBColumnReader<arrow::StringType>(typeInfoHelper, meta, pool);
        break;
    case EDataSlot::Int8:
        result = MakeFixedSizeDBColumnReader<DB::Int8, i8>(typeInfoHelper, meta, pool);
        break;
    case EDataSlot::Uint8:
    case EDataSlot::Bool:
        result = MakeFixedSizeDBColumnReader<DB::UInt8, ui8>(typeInfoHelper, meta, pool);
        break;
    case EDataSlot::Int16:
        result = MakeFixedSizeDBColumnReader<DB::Int16, i16>(typeInfoHelper, meta, pool);
        break;
    case EDataSlot::Uint16:
    case EDataSlot::Date:
        result = MakeFixedSizeDBColumnReader<DB::UInt16, ui16>(typeInfoHelper, meta, pool);
        break;
    case EDataSlot::Int32:
        result = MakeFixedSizeDBColumnReader<DB::Int32, i32>(typeInfoHelper, meta, pool);
        break;
    case EDataSlot::Uint32:
    case EDataSlot::Datetime:
        result = MakeFixedSizeDBColumnReader<DB::UInt32, ui32>(typeInfoHelper, meta, pool);
        break;
    case EDataSlot::Int64:
        result = MakeFixedSizeDBColumnReader<DB::Int64, i64>(typeInfoHelper, meta, pool);
        break;
    case EDataSlot::Uint64:
    case EDataSlot::Timestamp:
        result = MakeFixedSizeDBColumnReader<DB::UInt64, ui64>(typeInfoHelper, meta, pool);
        break;
    case EDataSlot::Float:
        result = MakeFixedSizeDBColumnReader<DB::Float32, float>(typeInfoHelper, meta, pool);
        break;
    case EDataSlot::Double:
        result = MakeFixedSizeDBColumnReader<DB::Float64, double>(typeInfoHelper, meta, pool);
        break;
    default:
        throw yexception() << "Unsupported arrow type";
    }
    return result;
}

template <typename TCH, typename TData>
class TFixedSizeDBColumnBuilder : public IDBColumnWriter {
public:
    TFixedSizeDBColumnBuilder(const TColumnMeta& meta)
        : IsOptional(meta.IsOptional)
    {
    }

    size_t Write(DB::IColumn& col, const arrow::ArrayData& input) final {
        const auto length = static_cast<size_t>(input.length);

        DB::IColumn* out = &col;
        DB::NullMap* nullMapData = nullptr;
        if (IsOptional) {
            auto nullable = static_cast<DB::ColumnNullable*>(out);
            out = &nullable->getNestedColumn();
            nullMapData = &nullable->getNullMapData();
        }

        auto& outData = dynamic_cast<DB::ColumnVector<TCH>*>(out)->getData();

        const TData *begin = input.GetValues<TData>(1);
        Y_ENSURE(begin);
        const TData *end = begin + length;
        outData.insert(outData.end(), begin, end);

        if (nullMapData) {
            for (size_t i = 0; i < length; ++i) {
                nullMapData->push_back(IsNull(input, i));
            }
        }

        return (sizeof(TData) + size_t(nullMapData != nullptr)) * length;
    }

private:
    const bool IsOptional;
};


template <typename TCH, typename TStringType, bool Nullable>
class TStringDBColumnBuilder : public IDBColumnWriter {
public:
    TStringDBColumnBuilder(const TColumnMeta& meta)
    {
        Y_UNUSED(meta);
    }

    size_t Write(DB::IColumn& col, const arrow::ArrayData& input) final {
        const auto length = static_cast<size_t>(input.length);
        DB::IColumn* out = &col;
        DB::NullMap* nullMapData = nullptr;
        if constexpr (Nullable) {
            auto nullable = static_cast<DB::ColumnNullable*>(out);
            out = &nullable->getNestedColumn();
            nullMapData = &nullable->getNullMapData();
        }

        size_t outSize = 0;
        TStringBlockReader<TStringType, Nullable> reader;
        for (size_t i = 0; i < length; ++i) {
            auto item = reader.GetItem(input, i);
            if constexpr (Nullable) {
                if (!item) {
                    nullMapData->push_back(1);
                    out->insertDefault();
                    continue;
                }
                nullMapData->push_back(0);
            }
            auto str = item.AsStringRef();
            out->insertData(str.Data(), str.Size());
            outSize += str.Size();
        }

        return outSize + (4 + size_t(Nullable)) * length;
    }
};

class TTupleDBColumnBuilder : public IDBColumnWriter {
public:
    TTupleDBColumnBuilder(const TColumnMeta& meta)
        : IsOptional(meta.IsOptional)
    {
        for (auto& child : meta.Items) {
            Children.push_back(IDBColumnWriter::Make(child));
        }
    }

    size_t Write(DB::IColumn& col, const arrow::ArrayData &input) final {
        const auto length = static_cast<size_t>(input.length);
        DB::IColumn* out = &col;
        DB::NullMap* nullMapData = nullptr;
        if (IsOptional) {
            auto nullable = static_cast<DB::ColumnNullable*>(out);
            out = &nullable->getNestedColumn();
            nullMapData = &nullable->getNullMapData();
        }

        size_t outSize = 0;
        if (nullMapData) {
            for (size_t i = 0; i < length; ++i) {
                nullMapData->push_back(IsNull(input, i));
            }
            outSize += length;
        }

        DB::ColumnTuple *outTuple = static_cast<DB::ColumnTuple*>(out);
        for (size_t i = 0; i < Children.size(); ++i) {
            outSize += Children[i]->Write(outTuple->getColumn(i), *input.child_data[i]);
        }
        return outSize;
    }
private:
    const bool IsOptional;
    TVector<IDBColumnWriter::TPtr> Children;
};

IDBColumnWriter::TPtr IDBColumnWriter::Make(const TColumnMeta& meta) {
    Y_ENSURE(meta.ArrowType);

    if (meta.IsTuple) {
        return std::make_shared<TTupleDBColumnBuilder>(meta);
    }

    Y_ENSURE(meta.Slot);
    IDBColumnWriter::TPtr result;

    switch (*meta.Slot) {
    case EDataSlot::String:
    case EDataSlot::Yson:
        if (meta.IsOptional) {
            result = std::make_shared<TStringDBColumnBuilder<DB::String, arrow::BinaryType, true>>(meta);
        } else {
            result = std::make_shared<TStringDBColumnBuilder<DB::String, arrow::BinaryType, false>>(meta);
        }
        break;
    case EDataSlot::Utf8:
    case EDataSlot::Json:
        if (meta.IsOptional) {
            result = std::make_shared<TStringDBColumnBuilder<DB::String, arrow::StringType, true>>(meta);
        } else {
            result = std::make_shared<TStringDBColumnBuilder<DB::String, arrow::StringType, false>>(meta);
        }
        break;
    case EDataSlot::Int8:
        result = std::make_shared<TFixedSizeDBColumnBuilder<DB::Int8, i8>>(meta);
        break;
    case EDataSlot::Uint8:
    case EDataSlot::Bool:
        result = std::make_shared<TFixedSizeDBColumnBuilder<DB::UInt8, ui8>>(meta);
        break;
    case EDataSlot::Int16:
        result = std::make_shared<TFixedSizeDBColumnBuilder<DB::Int16, i16>>(meta);
        break;
    case EDataSlot::Uint16:
    case EDataSlot::Date:
        result = std::make_shared<TFixedSizeDBColumnBuilder<DB::UInt16, ui16>>(meta);
        break;
    case EDataSlot::Int32:
        result = std::make_shared<TFixedSizeDBColumnBuilder<DB::Int32, i32>>(meta);
        break;
    case EDataSlot::Uint32:
    case EDataSlot::Datetime:
        result = std::make_shared<TFixedSizeDBColumnBuilder<DB::UInt32, ui32>>(meta);
        break;
    case EDataSlot::Int64:
        result = std::make_shared<TFixedSizeDBColumnBuilder<DB::Int64, i64>>(meta);
        break;
    case EDataSlot::Uint64:
    case EDataSlot::Timestamp:
        result = std::make_shared<TFixedSizeDBColumnBuilder<DB::UInt64, ui64>>(meta);
        break;
    case EDataSlot::Float:
        result = std::make_shared<TFixedSizeDBColumnBuilder<DB::Float32, float>>(meta);
        break;
    case EDataSlot::Double:
        result = std::make_shared<TFixedSizeDBColumnBuilder<DB::Float64, double>>(meta);
        break;
    default:
        throw yexception() << "Unsupported arrow type";
    }

    return result;
}

ui64 ConvertInputValue(DB::IColumn* col, const TColumnMeta& meta, const TUnboxedValuePod& value, const IValueBuilder* valueBuilder) {
    DB::IColumn* data = col;
    if (meta.IsScalarBlock.has_value()) {
        Y_ENSURE(meta.Writer);
        bool isScalar;
        ui64 length;
        ui32 chunks = valueBuilder->GetArrowBlockChunks(value, isScalar, length);
        meta.Writer->Reserve(*col, length);
        ui64 size = 0;
        for (ui32 chunk = 0; chunk < chunks; ++chunk) {
            ArrowArray a;
            valueBuilder->ExportArrowBlock(value, chunk, &a);
            auto arr = ARROW_RESULT(arrow::ImportArray(&a, meta.ArrowType));
            size += meta.Writer->Write(*col, *arr->data());
        }
        return size;
    }

    if (!meta.Enum.empty()) {
        ui32 varIndex = value.GetVariantIndex();
        if (col->getDataType() == DB::TypeIndex::Int8) {
            i8 x = i8(Min<i8>() + varIndex);
            data->insertData((const char*)&x, sizeof(x));
            return 1;
        } else if (col->getDataType() == DB::TypeIndex::Int16) {
            i16 x = i16(Min<i16>() + varIndex);
            data->insertData((const char*)&x, sizeof(x));
            return 2;
        } else {
            throw yexception() << "Unsupported column type: " << col->getName();
        }
    }

    if (meta.Aggregation) {
        auto& aggColumn = static_cast<DB::ColumnAggregateFunction&>(*col);
        DB::Field field = DB::AggregateFunctionStateData();
        auto func = aggColumn.getAggregateFunction();
        auto typeString = DB::DataTypeAggregateFunction(func, func->getArgumentTypes(), func->getParameters()).getName();
        field.safeGet<DB::AggregateFunctionStateData>().name = typeString;
        auto ref = value.AsStringRef();
        field.safeGet<DB::AggregateFunctionStateData>().data.assign(ref.Data(), ref.Data() + ref.Size());
        aggColumn.insert(field);
        return 8 + ref.Size();
    }

    if (meta.IsEmptyList) {
        data->insertDefault();
        return 0;
    }

    if (meta.IsList) {
        ui64 bytes = 0;
        DB::ColumnArray& res = static_cast<DB::ColumnArray&>(*col);
        DB::IColumn::Offsets& offsets = res.getOffsets();
        data = &res.getData();
        TUnboxedValue iterator = value.GetListIterator();
        ui64 count = 0;
        const auto& childMeta = meta.Items.front();
        for (;;) {
            TUnboxedValue current;
            if (!iterator.Next(current)) {
                break;
            }

            ++count;
            bytes += ConvertInputValue(data, childMeta, current, valueBuilder);
        }

        auto prev = offsets.back();
        offsets.push_back(prev + count);
        return bytes;
    }

    if (meta.IsTuple) {
        ui64 bytes = 0;
        DB::ColumnTuple& res = static_cast<DB::ColumnTuple&>(*col);
        for (ui32 i = 0; i < meta.Items.size(); ++i) {
            auto current = value.GetElement(i);
            bytes += ConvertInputValue(&res.getColumn(i), meta.Items[i], current, valueBuilder);
        }

        return bytes;
    }

    if (!meta.Slot) {
        data->insertDefault();
        return 0;
    }

    if (meta.IsOptional && !value) {
        data->insertData(nullptr, 0);
        return 1 + 4;
    } else if (GetDataTypeInfo(*meta.Slot).Features & NUdf::StringType) {
        auto ref = value.AsStringRef();
        data->insertData(ref.Data(), ref.Size());
        return 4 + ref.Size();
    } else if (*meta.Slot == EDataSlot::Uuid) {
        auto ref = value.AsStringRef();
        Y_ENSURE(ref.Size() == 16);
        char uuid[16];
        PermuteUuid(ref.Data(), uuid, true);
        data->insertData(uuid, sizeof(uuid));
        return 16;
    } else {
        auto size = GetDataTypeInfo(*meta.Slot).FixedSize;
        data->insertData((const char*)&value, size);
        return size;
    }
}

class TSource : public DB::ISource {
public:
    TSource(const DB::Block & sampleBlock, size_t maxBlockSize, ui64 preferredBlockSizeBytes,
        DB::ContextPtr context, const std::vector<TColumnMeta>& metaColumns)
        : ISource(sampleBlock)
        , SampleBlock(sampleBlock)
        , MaxBlockSize(maxBlockSize)
        , PreferredBlockSizeBytes(preferredBlockSizeBytes)
        , HostContext(GetHostContext(context))
        , MetaColumns(metaColumns)
    {
    }

    DB::String getName() const override {
        return "Yql";
    }

    std::optional<DB::Chunk> tryGenerate() override {
        if (IsFinished) {
            return {};
        }

        if (!InputValue) {
            InputValue = HostContext->InputValue;
            ValueBuilder = HostContext->ValueBuilder;
        }

        auto mutableColumns = SampleBlock.cloneEmptyColumns();
        size_t rows = 0;
        ui64 bytes = 0;
        for (;;) {
            TUnboxedValue row;
            auto status = InputValue.Fetch(row);
            if (status == EFetchStatus::Finish) {
                IsFinished = true;
                break;
            }

            Y_ENSURE(status == EFetchStatus::Ok);
            auto elements = row.GetElements();
            for (ui32 i = 0; i < MetaColumns.size(); ++i) {
                if (elements) {
                    bytes += ConvertInputValue(mutableColumns[i].get(), MetaColumns[i], elements[i], ValueBuilder);
                } else {
                    bytes += ConvertInputValue(mutableColumns[i].get(), MetaColumns[i], row.GetElement(i), ValueBuilder);
                }
            }

            ++rows;
            if (rows >= MaxBlockSize || bytes >= PreferredBlockSizeBytes) {
                break;
            }
        }

        if (!rows) {
            return {};
        }

        DB::Chunk chunk;
        chunk.setColumns(std::move(mutableColumns), rows);
        return chunk;
    }

private:
    const DB::Block SampleBlock;
    const size_t MaxBlockSize;
    const ui64 PreferredBlockSizeBytes;
    std::shared_ptr<const THostContext> HostContext;
    const std::vector<TColumnMeta> MetaColumns;
    TUnboxedValue InputValue;
    const IValueBuilder* ValueBuilder = nullptr;
    bool IsFinished = false;
};

void FillArrowTypeSingle(TColumnMeta& meta, const TType* type, const ITypeInfoHelper::TPtr& typeHelper) {
    auto arrowTypeHandle = typeHelper->MakeArrowType(type);
    if (arrowTypeHandle) {
        ArrowSchema s;
        arrowTypeHandle->Export(&s);
        meta.ArrowType = ARROW_RESULT(arrow::ImportType(&s));
        meta.HostArrowType.reset(arrowTypeHandle.Release());
        meta.MaxBlockLen = typeHelper->GetMaxBlockLength(type);
    }
}

void FillArrowType(TColumnMeta& meta, const TType* type, const ITypeInfoHelper::TPtr& typeHelper) {
    FillArrowTypeSingle(meta, type, typeHelper);
    auto listInspector = TListTypeInspector(*typeHelper, type);
    if (listInspector) {
        auto innerType = listInspector.GetItemType();
        Y_ENSURE(meta.Items.size() == 1);
        return FillArrowType(meta.Items.front(), innerType, typeHelper);
    }

    auto tupleInspector = TTupleTypeInspector(*typeHelper, type);
    if (tupleInspector) {
        Y_ENSURE(meta.Items.size() == tupleInspector.GetElementsCount());
        for (ui32 i = 0; i < tupleInspector.GetElementsCount(); ++i) {
            FillArrowType(meta.Items[i], tupleInspector.GetElementType(i), typeHelper);
        }
        return;
    }
}

TString FormatType(const TType* type, const ITypeInfoHelper::TPtr& typeHelper) {
    TTypePrinter printer(*typeHelper, type);
    TStringStream out;
    printer.Out(out);
    return out.Str();
}

TColumnMeta MakeMeta(const TType* type, const ITypeInfoHelper::TPtr& typeHelper) {
    TColumnMeta ret;
    auto blockInspector = TBlockTypeInspector(*typeHelper, type);
    if (blockInspector) {
        type = blockInspector.GetItemType();
        ret.IsScalarBlock = blockInspector.IsScalar();
    }

    const TType* originalType = type;
    FillArrowTypeSingle(ret, type, typeHelper);

    auto varInspector = TVariantTypeInspector(*typeHelper, type);
    if (varInspector) {
        auto underlying = varInspector.GetUnderlyingType();
        auto structInspector = TStructTypeInspector(*typeHelper, underlying);
        if (!structInspector) {
            throw yexception() << "Expected struct variant, but got: " << FormatType(originalType, typeHelper);
        }

        for (ui32 i = 0; i < structInspector.GetMembersCount(); ++i) {
            auto memberType = structInspector.GetMemberType(i);
            if (typeHelper->GetTypeKind(memberType) != ETypeKind::Void) {
                throw yexception() << "Expected only enum, but member '" << structInspector.GetMemberName(i) << "' has type: " << FormatType(memberType, typeHelper);
            }

            ret.Enum.push_back(TString(structInspector.GetMemberName(i)));
        }

        return ret;
    }

    auto taggedInspector = TTaggedTypeInspector(*typeHelper, type);
    if (taggedInspector) {
        ret.Aggregation = taggedInspector.GetTag();
        if (!ret.Aggregation->StartsWith("AggregateFunction(") || !ret.Aggregation->EndsWith(")")) {
            throw yexception() << "Unsupported tag: " << *ret.Aggregation << " for type: " << FormatType(originalType, typeHelper);
        }

        type = taggedInspector.GetBaseType();
        auto dataInspector = TDataTypeInspector(*typeHelper, type);
        if (!dataInspector || dataInspector.GetTypeId() != TDataType<const char*>::Id) {
            throw yexception() << "Expected only tagged String, but got: " << FormatType(originalType, typeHelper);
        }

        return ret;
    }

    if (typeHelper->GetTypeKind(type) == ETypeKind::EmptyList) {
        ret.IsEmptyList = true;
        return ret;
    }

    auto listInspector = TListTypeInspector(*typeHelper, type);
    if (listInspector) {
        ret.IsList = true;
        auto innerType = listInspector.GetItemType();
        ret.Items.emplace_back(MakeMeta(innerType, typeHelper));
        return ret;
    }

    auto tupleInspector = TTupleTypeInspector(*typeHelper, type);
    if (tupleInspector) {
        ret.IsTuple = true;
        for (ui32 i = 0; i < tupleInspector.GetElementsCount(); ++i) {
            ret.Items.emplace_back(MakeMeta(tupleInspector.GetElementType(i), typeHelper));
        }

        return ret;
    }

    if (typeHelper->GetTypeKind(type) == ETypeKind::Null) {
        return ret;
    }

    auto optInspector = TOptionalTypeInspector(*typeHelper, type);
    if (optInspector) {
        ret.IsOptional = true;
        type = optInspector.GetItemType();
    }

    auto dataInspector = TDataTypeInspector(*typeHelper, type);
    if (!dataInspector) {
        throw yexception() << "Unsupported input type: " << FormatType(originalType, typeHelper);
    }

    ret.Slot = GetDataSlot(dataInspector.GetTypeId());
    return ret;
}

DB::DataTypePtr MetaToClickHouse(const TColumnMeta& meta) {
    if (!meta.Enum.empty()) {
        if (meta.Enum.size() < 256) {
            DB::DataTypeEnum8::Values values;
            if (!meta.EnumVar.size()) {
                for (ui32 i = 0; i < meta.Enum.size(); ++i) {
                    std::string name(meta.Enum[i]);
                    values.emplace_back(std::make_pair(name, i8(Min<i8>() + i)));
                }
            }
            else {
                for (const auto& x : meta.EnumVar) {
                    std::string name(x.second.first);
                    values.emplace_back(std::make_pair(name, i8(x.first)));
                }
            }

            return std::make_shared<DB::DataTypeEnum8>(values);
        }
        else {
            DB::DataTypeEnum16::Values values;
            if (!meta.EnumVar.size()) {
                for (ui32 i = 0; i < meta.Enum.size(); ++i) {
                    std::string name(meta.Enum[i]);
                    values.emplace_back(std::make_pair(name, i16(Min<i16>() + i)));
                }
            }
            else {
                for (const auto& x : meta.EnumVar) {
                    std::string name(x.second.first);
                    values.emplace_back(std::make_pair(name, x.first));
                }
            }

            return std::make_shared<DB::DataTypeEnum16>(values);
        }
    }

    if (meta.Aggregation) {
        return DB::DataTypeFactory::instance().get(*meta.Aggregation);
    }

    if (meta.IsEmptyList) {
        return std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeNothing>());
    }

    if (meta.IsList) {
        return std::make_shared<DB::DataTypeArray>(MetaToClickHouse(meta.Items.front()));
    }

    if (meta.IsTuple) {
        DB::DataTypes elems;
        for (const auto& e : meta.Items) {
            elems.push_back(MetaToClickHouse(e));
        }

        return std::make_shared<DB::DataTypeTuple>(elems);
    }

    DB::DataTypePtr ret;
    if (!meta.Slot) {
        ret = makeNullable(std::make_shared<DB::DataTypeNothing>());
    }
    else {
        switch (*meta.Slot) {
        case EDataSlot::Int8:
            ret = std::make_shared<DB::DataTypeInt8>();
            break;
        case EDataSlot::Uint8:
            ret = std::make_shared<DB::DataTypeUInt8>();
            break;
        case EDataSlot::Int16:
            ret = std::make_shared<DB::DataTypeInt16>();
            break;
        case EDataSlot::Uint16:
            ret = std::make_shared<DB::DataTypeUInt16>();
            break;
        case EDataSlot::Int32:
            ret = std::make_shared<DB::DataTypeInt32>();
            break;
        case EDataSlot::Uint32:
            ret = std::make_shared<DB::DataTypeUInt32>();
            break;
        case EDataSlot::Int64:
            ret = std::make_shared<DB::DataTypeInt64>();
            break;
        case EDataSlot::Uint64:
            ret = std::make_shared<DB::DataTypeUInt64>();
            break;
        case EDataSlot::Float:
            ret = std::make_shared<DB::DataTypeFloat32>();
            break;
        case EDataSlot::Double:
            ret = std::make_shared<DB::DataTypeFloat64>();
            break;
        case EDataSlot::String:
        case EDataSlot::Utf8:
        case EDataSlot::Yson:
        case EDataSlot::Json:
            ret = std::make_shared<DB::DataTypeString>();
            break;
        case EDataSlot::Date:
            ret = std::make_shared<DB::DataTypeDate>();
            break;
        case EDataSlot::Datetime:
            ret = std::make_shared<DB::DataTypeDateTime>();
            break;
        case EDataSlot::Uuid:
            ret = std::make_shared<DB::DataTypeUUID>();
            break;
        default:
            throw yexception() << "Unsupported argument type: " << GetDataTypeInfo(*meta.Slot).Name;
        }
    }

    ret = meta.IsOptional ? makeNullable(ret) : ret;
    return ret;
}

class TStorageInput : public DB::IStorage
{
public:
    TStorageInput(DB::ContextPtr context)
        : IStorage(DB::StorageID("_local", "Input"))
    {
        DB::NamesAndTypesList columns;
        auto hostCtx = GetHostContext(context);
        auto inspector = TStructTypeInspector(*hostCtx->TypeHelper, hostCtx->InputType);
        Y_ENSURE(inspector);
        for (ui32 i = 0; i < inspector.GetMembersCount(); ++i) {
            auto memberType = inspector.GetMemberType(i);
            auto memberName = inspector.GetMemberName(i);
            auto meta = MakeMeta(memberType, hostCtx->TypeHelper);
            MetaColumns.push_back(meta);
            columns.push_back({ std::string(memberName.Data(), memberName.Size()), MetaToClickHouse(meta) });
        }

        DB::StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(DB::ColumnsDescription(columns));
        setInMemoryMetadata(storage_metadata);
    }

    std::string getName() const override { return "Input"; }

    void startup() override {
    }

    DB::Pipe read(
        const DB::Names & column_names,
        const DB::StorageSnapshotPtr & storage_snapshot,
        DB::SelectQueryInfo & query_info,
        DB::ContextPtr context,
        DB::QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        Y_UNUSED(column_names);
        Y_UNUSED(query_info);
        Y_UNUSED(processed_stage);
        Y_UNUSED(num_streams);
        auto metadata_snapshot = storage_snapshot->metadata;
        DB::Block sampleBlock = metadata_snapshot->getSampleBlock();
        DB::Pipes pipes;
        auto hostCtx = GetHostContext(context);
        auto source = std::make_shared<TSource>(sampleBlock, max_block_size,
            context->getSettingsRef()[DB::Setting::preferred_block_size_bytes], context, MetaColumns);
        return DB::Pipe(source);
    }

private:
    std::vector<TColumnMeta> MetaColumns;
};

class TLocalDatabase : public DB::IDatabase {
public:
    TLocalDatabase(const std::string& name)
        : IDatabase(name)
    {}

    DB::String getEngineName() const override {
        return "Local";
    }

    bool isTableExist(const DB::String& name, DB::ContextPtr context) const override {
        const auto& hostContext = GetHostContext(context);
        if (!hostContext) {
            return false;
        }
        return hostContext->InputType && name == "Input";
    }

    DB::StoragePtr tryGetTable(const DB::String& name, DB::ContextPtr context) const override {
        const auto& hostContext = GetHostContext(context);
        if (!hostContext) {
            return nullptr;
        }
        if (hostContext->InputType && name == "Input") {
            return std::make_shared<TStorageInput>(context);
        }

        return nullptr;
    }

    DB::DatabaseTablesIteratorPtr getTablesIterator(DB::ContextPtr context,
        const FilterByNameFunction& filter_by_table_name = {},
        bool skip_not_loaded = false) const override {
        Y_UNUSED(context);
        Y_UNUSED(filter_by_table_name);
        Y_UNUSED(skip_not_loaded);
        throw yexception() << "Not impl";
    }

    bool empty() const override {
        return false;
    }

    DB::ASTPtr getCreateDatabaseQuery() const override {
        throw yexception() << "Not impl";
    }

    void shutdown() override {
    }
};

struct TCHContext {
    using TWeakPtr = std::weak_ptr<TCHContext>;
    using TPtr = std::shared_ptr<TCHContext>;

    TCHContext()
        : SharedContext(DB::Context::createShared())
        , Context(DB::Context::createGlobal(SharedContext.get()))
    {
        App.config().add(new TAppConfig(), false);
        Y_UNUSED(UseSSL);
        Context->makeGlobalContext();
        Context->setConfig(DB::ConfigurationPtr(new TServerConfig()));
        Context->setPath(TmpDir.Path().c_str());

        DB::registerFunctions();
        DB::registerFormats();
        DB::registerAggregateFunctions();
        DB::registerTableFunctions(/*use_legacy_mongodb_integration*/ false);
        DB::registerInterpreters();
        std::string defaultDatabase = "_local";
        auto db = std::make_shared<TLocalDatabase>(defaultDatabase);
        DB::DatabaseCatalog::instance().attachDatabase(defaultDatabase, db);
        Context->setCurrentDatabase(defaultDatabase);

        auto systemDatabase = std::make_shared<DB::DatabaseMemory>("system", Context);
        DB::DatabaseCatalog::instance().attachDatabase("system", systemDatabase);
        DB::attachSystemTablesServer(Context, *systemDatabase, /*has_zookeeper*/ false);
    }

    void InitPollingThread() {
        const std::unique_lock lock(QueueGuard);
        if (PollingQueue) {
            return;
        }

        PollingQueue.emplace();
        PollingThread = std::thread(&PollingThreadThunk, this);
    }

    ~TCHContext() {
        if (PollingQueue) {
            PollingQueue->finish();
        }
        if (PollingThread.joinable()) {
            PollingThread.join();
        }
    }

    static void PollingThreadThunk(void* param) {
        static_cast<TCHContext*>(param)->PollingThreadProc();
    }

    void PollingThreadProc() {
        for (;;) {
            INotify* notify = nullptr;
            {
                std::unique_lock lock(QueueGuard);
                auto task = PollingQueue->wait(lock);
                if (!task) {
                    break;
                }

                auto notifyKey = (ui64)task.data;
                auto it = NotifySet.find(notifyKey);
                if (it != NotifySet.end()) {
                    notify = it->second;
                }
            }

            if (notify) {
                notify->Do();
            }
        }
    }

    void RemoveNotify(ui64 notifyKey) {
        std::unique_lock lock(QueueGuard);
        NotifySet.erase(notifyKey);
    }

    ui64 AllocateNotifyKey() {
        std::unique_lock lock(QueueGuard);
        return ++NextNotifyKey;
    }

    void Wait(ui64 notifyKey, INotify* stream, int fd) {
        std::unique_lock lock(QueueGuard);
        Y_ENSURE(PollingQueue);
        if (!NotifySet.contains(notifyKey)) {
            NotifySet.insert(std::make_pair(notifyKey, stream));
            PollingQueue->addTask(0, (void*)notifyKey, fd);
        }
    }

    TApp App;
    DB::UseSSL UseSSL;
    DB::SharedContextHolder SharedContext;
    DB::ContextMutablePtr Context;

    std::mutex QueueGuard;
    ui64 NextNotifyKey = 0;
    std::unordered_map<ui64, INotify*> NotifySet;
    std::optional<DB::PollingQueue> PollingQueue;
    std::thread PollingThread; // should be destroyed first
    TTempDir TmpDir;
};

DB::ColumnPtr MakeConstColumn(const TString& type, const TString& value) {
    DB::ColumnPtr column;
    if (type == "Int8") {
        column = DB::ColumnInt8::create(1, FromString<i8>(value));
    } else if (type == "Uint8") {
        column = DB::ColumnUInt8::create(1, FromString<ui8>(value));
    } else if (type == "Int16") {
        column = DB::ColumnInt16::create(1, FromString<i16>(value));
    } else if (type == "Uint16") {
        column = DB::ColumnUInt16::create(1, FromString<ui16>(value));
    } else if (type == "Int32") {
        column = DB::ColumnInt32::create(1, FromString<i32>(value));
    } else if (type == "Uint32") {
        column = DB::ColumnUInt32::create(1, FromString<ui32>(value));
    } else if (type == "Int64") {
        column = DB::ColumnInt64::create(1, FromString<i64>(value));
    } else if (type == "Uint64") {
        column = DB::ColumnUInt64::create(1, FromString<ui64>(value));
    } else if (type == "Float") {
        column = DB::ColumnFloat32::create(1, FromString<float>(value));
    } else if (type == "Double") {
        column = DB::ColumnFloat64::create(1, FromString<double>(value));
    } else if (type == "String") {
        auto mut = DB::ColumnString::create();
        mut->insert(DB::Field(std::string(value)));
        column = std::move(mut);
    } else {
        throw yexception() << "Unsupported const type: " << type;
    }

    return DB::ColumnConst::create(column, 1);
}

TUnboxedValuePod ConvertOutputValue(const DB::IColumn* col, const TColumnMeta& meta, ui32 tzId,
    const IValueBuilder* valueBuilder, ssize_t externalIndex = 0)
{
    if (meta.IsScalarBlock.has_value()) {
        Y_ENSURE(meta.Reader);
        arrow::Datum outDatum = meta.Reader->Read(*col);
        std::vector<ArrowArray> arrowArrays;
        ForEachArrayData(outDatum, [&](const std::shared_ptr<arrow::ArrayData>& arrayData) {
            std::shared_ptr<arrow::Array> arr = arrow::Datum(arrayData).make_array();
            arrowArrays.emplace_back();
            ARROW_OK(arrow::ExportArray(*arr, &arrowArrays.back()));
        });
        return valueBuilder->ImportArrowBlock(arrowArrays.data(), arrowArrays.size(), *meta.IsScalarBlock, *meta.HostArrowType).Release();
    }

    if (!meta.Enum.empty()) {
        auto ref = col->getDataAt(externalIndex);
        i16 value;
        if (col->getDataType() == DB::TypeIndex::Int8) {
            value = *(const i8*)ref.data;
        } else if (col->getDataType() == DB::TypeIndex::Int16) {
            value = *(const i16*)ref.data;
        } else {
            throw yexception() << "Unsupported column type: " << col->getName();
        }

        Y_ENSURE(meta.EnumVar.size() == meta.Enum.size());
        const auto x = meta.EnumVar.find(value);
        if (x == meta.EnumVar.cend()) {
            throw yexception() << "Missing enum value: " << value;
        }

        const ui32 index = x->second.second;
        return valueBuilder->NewVariant(index, TUnboxedValue::Void()).Release();
    }

    if (meta.Aggregation) {
        auto field = (*col)[externalIndex];
        const auto& state = field.safeGet<DB::AggregateFunctionStateData>();
        return valueBuilder->NewString({ state.data.data(), (ui32)state.data.size() }).Release();
    }

    if (meta.IsEmptyList) {
        return valueBuilder->NewEmptyList().Release();
    }

    const DB::IColumn* data = col;
    if (meta.IsList) {
        const DB::ColumnArray& res = static_cast<const DB::ColumnArray&>(*col);
        const DB::IColumn::Offsets& offsets = res.getOffsets();
        data = &res.getData();
        ui64 start = offsets[externalIndex - 1];
        ui64 limit = offsets[externalIndex] - start;
        TUnboxedValue* items = nullptr;
        TUnboxedValue ret = valueBuilder->NewArray(limit, items);
        for (ui64 index = start; index < start + limit; ++index) {
            TUnboxedValue* current = items + index - start;
            *current = ConvertOutputValue(data, meta.Items.front(), tzId, valueBuilder, index);
        }

        return ret.Release();
    }

    if (meta.IsTuple) {
        auto count = meta.Items.size();
        TUnboxedValue* items = nullptr;
        TUnboxedValue ret = valueBuilder->NewArray(count, items);
        const DB::ColumnTuple& res = static_cast<const DB::ColumnTuple&>(*col);
        for (ui32 i = 0; i < count; ++i) {
            items[i] = ConvertOutputValue(&res.getColumn(i), meta.Items[i], tzId, valueBuilder, externalIndex);
        }

        return ret.Release();
    }

    if (!meta.Slot) {
        return {};
    }

    StringRef ref;
    if (meta.IsOptional) {
        if (data->isNullAt(externalIndex)) {
            return {};
        }

        ref = static_cast<const DB::ColumnNullable*>(data)->getNestedColumn().getDataAt(externalIndex);
    } else {
        ref = data->getDataAt(externalIndex);
    }

    if (const auto slot = *meta.Slot; slot == EDataSlot::String) {
        return valueBuilder->NewString({ ref.data, (ui32)ref.size }).Release();
    } else if (slot == EDataSlot::Utf8) {
        if (!IsUtf8(std::string_view(ref))) {
            ythrow yexception() << "Bad Utf8.";
        }
        return valueBuilder->NewString({ ref.data, (ui32)ref.size }).Release();
    } else if (slot == EDataSlot::Uuid) {
        char uuid[16];
        PermuteUuid(ref.data, uuid, false);
        return valueBuilder->NewString({ uuid, sizeof(uuid) }).Release();
    } else if (slot == EDataSlot::Decimal) {
        if (NDecimal::TInt128 decimal; ref.size == sizeof(decimal)) {
            std::memcpy(&decimal, ref.data, ref.size);
            return TUnboxedValuePod(decimal);
        } else
            ythrow yexception() << "Unsupported decimal of size " << ref.size;
    } else {
        auto size = GetDataTypeInfo(*meta.Slot).FixedSize;
        TUnboxedValuePod ret = TUnboxedValuePod::Zero();
        memcpy(&ret, ref.data, size);
        if (tzId) {
            if (*meta.Slot == EDataSlot::TzDatetime) {
                ret.SetTimezoneId(tzId);
            } else if (*meta.Slot == EDataSlot::TzDate) {
                auto& builder = valueBuilder->GetDateBuilder();
                ui32 year, month, day;
                if (const auto date = ret.Get<ui16>(); !builder.SplitDate(date, year, month, day)) {
                    ythrow yexception() << "Error in SplitDate(" << date << ").";
                }

                ui32 datetime;
                if (!builder.MakeDatetime(year, month, day, 23u, 59u, 0u, datetime, tzId)) {
                    ythrow yexception() << "Error in MakeDatetime(" << year << ',' << month << ',' << day << ',' << tzId << ").";
                }

                ret = TUnboxedValuePod(ui16(datetime / 86400u));
                ret.SetTimezoneId(tzId);
            }
        }

        return ret;
    }
}

class TImpl : public TBoxedValue {
public:
    TImpl(DB::FunctionBasePtr func, const DB::ColumnsWithTypeAndName& arguments,
        TColumnMeta retType, const std::vector<TColumnMeta>& argsTypes,
        const TSourcePosition& pos)
        : Func(func)
        , Arguments(arguments)
        , RetType(retType)
        , ArgsTypes(argsTypes)
        , Pos(pos)
    {
        DB::ColumnsWithTypeAndName columns;
        for (ui32 i = 0; i < ArgsTypes.size(); ++i) {
            auto type = Func->getArgumentTypes()[i];
            DB::ColumnPtr columnValue = Arguments[i].column ? arguments[i].column : type->createColumn();
            columns.push_back(DB::ColumnWithTypeAndName(columnValue, type, TStringBuilder() << "arg" << i));
        }

        Columns = std::move(columns);
        Prepared = Func->prepare(Columns);
    }

    TUnboxedValue Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const final
    {
        try {
            auto columns = Columns;
            size_t rowsCount = 1;
            for (ui32 i=0; i < ArgsTypes.size(); ++i) {
                if (Arguments[i].column) {
                    columns[i].column = Arguments[i].column;
                } else {
                    auto column = columns[i].type->createColumn();
                    ConvertInputValue(column.get(), ArgsTypes[i], args[i], valueBuilder);
                    if (!ArgsTypes[i].IsScalarBlock) {
                        column = DB::ColumnConst::create(std::move(column), 1);
                    } else {
                        rowsCount = Max(rowsCount, column->size());
                    }

                    columns[i].column = std::move(column);
                }
            }

            for (ui32 i = 0; i < ArgsTypes.size(); ++i) {
                if (Arguments[i].column) {
                    Y_ENSURE(DB::isColumnConst(*Arguments[i].column));
                    auto inner = assert_cast<const DB::ColumnConst &>(*Arguments[i].column).getDataColumnPtr();
                    columns[i].column = DB::ColumnConst::create(inner, rowsCount);
                } else if (ArgsTypes[i].IsScalarBlock && *ArgsTypes[i].IsScalarBlock) {
                    columns[i].column = DB::ColumnConst::create(std::move(columns[i].column), rowsCount);
                }
            }

            auto output = Prepared->execute(columns, Func->getResultType(), rowsCount, false);
            while (auto constOutput = DB::checkAndGetColumn<DB::ColumnConst>(output.get())) {
                output = constOutput->getDataColumnPtr();
            }

            Y_ENSURE(output->size() == rowsCount);
            return ConvertOutputValue(output.get(), RetType, 0, valueBuilder);
        }
        catch (const DBPoco::Exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).c_str());
        }
        catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.what()).c_str());
        }
    }

private:
   DB::FunctionBasePtr Func;
   DB::ColumnsWithTypeAndName Arguments;
   TColumnMeta RetType;
   const std::vector<TColumnMeta> ArgsTypes;
   const TSourcePosition Pos;
   DB::ColumnsWithTypeAndName Columns;
   DB::ExecutableFunctionPtr Prepared;
};

TColumnMeta MetaFromClickHouse(const DB::DataTypePtr& type, bool withTz) {
   TColumnMeta ret;
   auto inner = type;
   if (inner->getTypeId() == DB::TypeIndex::Enum8) {
       const DB::DataTypeEnum8* enum8 = DB::checkAndGetDataType<DB::DataTypeEnum8>(inner.get());
       const auto& values = enum8->getValues();
       for (const auto& value : values) {
           ret.Enum.push_back(TString(value.first));
           ret.EnumVar[value.second] = std::make_pair(TString(value.first), 0);
       }
   }

   if (inner->getTypeId() == DB::TypeIndex::Enum16) {
       const DB::DataTypeEnum16* enum16 = DB::checkAndGetDataType<DB::DataTypeEnum16>(inner.get());
       const auto& values = enum16->getValues();
       for (const auto& value : values) {
           ret.Enum.push_back(TString(value.first));
           ret.EnumVar[value.second] = std::make_pair(TString(value.first), 0);
       }
   }

   if (inner->getTypeId() == DB::TypeIndex::Enum8 || inner->getTypeId() == DB::TypeIndex::Enum16) {
       Sort(ret.Enum);
       for (auto& x : ret.EnumVar) {
           const auto& name = x.second.first;
           auto sortedIndex = LowerBound(ret.Enum.begin(), ret.Enum.end(), name) - ret.Enum.begin();
           Y_ENSURE(size_t(sortedIndex) < ret.Enum.size());
           x.second.second = sortedIndex;
       }

       return ret;
   }

   if (inner->getTypeId() == DB::TypeIndex::AggregateFunction) {
       ret.Aggregation = inner->getName();
       return ret;
   }

   if (inner->getTypeId() == DB::TypeIndex::Array) {
       const DB::DataTypeArray* array = DB::checkAndGetDataType<DB::DataTypeArray>(inner.get());
       inner = array->getNestedType();
       if (inner->getTypeId() == DB::TypeIndex::Nothing) {
           ret.IsEmptyList = true;
           return ret;
       }

       ret.IsList = true;
       ret.Items.emplace_back(MetaFromClickHouse(inner, withTz));
       return ret;
   }

   if (inner->getTypeId() == DB::TypeIndex::Tuple) {
       const DB::DataTypeTuple* tuple = DB::checkAndGetDataType<DB::DataTypeTuple>(inner.get());
       const auto& elems = tuple->getElements();
       ret.IsTuple = true;
       for (const auto& e : elems) {
           ret.Items.emplace_back(MetaFromClickHouse(e, withTz));
       }

       return ret;
   }

   if (inner->isNullable()) {
       ret.IsOptional = true;
       inner = removeNullable(inner);
       if (inner->getTypeId() == DB::TypeIndex::Nothing) {
          return ret;
       }
   }

   if (inner->getTypeId() == DB::TypeIndex::UInt8) ret.Slot = EDataSlot::Uint8;
   else if (inner->getTypeId() == DB::TypeIndex::Int8) ret.Slot = EDataSlot::Int8;
   else if (inner->getTypeId() == DB::TypeIndex::UInt16) ret.Slot = EDataSlot::Uint16;
   else if (inner->getTypeId() == DB::TypeIndex::Int16) ret.Slot = EDataSlot::Int16;
   else if (inner->getTypeId() == DB::TypeIndex::UInt32) ret.Slot = EDataSlot::Uint32;
   else if (inner->getTypeId() == DB::TypeIndex::Int32) ret.Slot = EDataSlot::Int32;
   else if (inner->getTypeId() == DB::TypeIndex::UInt64) ret.Slot = EDataSlot::Uint64;
   else if (inner->getTypeId() == DB::TypeIndex::Int64) ret.Slot = EDataSlot::Int64;
   else if (inner->getTypeId() == DB::TypeIndex::Float32) ret.Slot = EDataSlot::Float;
   else if (inner->getTypeId() == DB::TypeIndex::Float64) ret.Slot = EDataSlot::Double;
   else if (inner->getTypeId() == DB::TypeIndex::String) ret.Slot = EDataSlot::String;
   else if (inner->getTypeId() == DB::TypeIndex::FixedString) ret.Slot = EDataSlot::String;
   else if (inner->getTypeId() == DB::TypeIndex::Date) ret.Slot = withTz ? EDataSlot::TzDate : EDataSlot::Date;
   else if (inner->getTypeId() == DB::TypeIndex::DateTime) ret.Slot = withTz ? EDataSlot::TzDatetime : EDataSlot::Datetime;
   else if (inner->getTypeId() == DB::TypeIndex::UUID) ret.Slot = EDataSlot::Uuid;
   else throw yexception() << "Unsupported return type: " << type->getName();

   return ret;
}

TType* MakeType(const TColumnMeta& meta, IFunctionTypeInfoBuilder& builder, bool withTz) {
    if (!meta.Enum.empty()) {
        auto enumBuilder = builder.Enum(meta.Enum.size());
        for (const auto&x : meta.Enum) {
            enumBuilder->AddField(x, nullptr);
        }

        return enumBuilder->Build();
    }

    if (meta.Aggregation) {
        return builder.Tagged(builder.SimpleType<char*>(), *meta.Aggregation);
    }

    if (meta.IsEmptyList) {
        return builder.EmptyList();
    }

    if (meta.IsList) {
        auto item = MakeType(meta.Items.front(), builder, withTz);
        return builder.List()->Item(item).Build();
    }

    if (meta.IsTuple) {
        auto tuple = builder.Tuple();
        for (const auto& e : meta.Items) {
            tuple->Add(MakeType(e, builder, withTz));
        }

        return tuple->Build();
    }

    if (!meta.Slot) {
        return builder.Null();
    }

    auto slot = *meta.Slot;
    if (withTz) {
        if (slot == EDataSlot::Date) {
           slot = EDataSlot::TzDate;
        } else if (slot == EDataSlot::Datetime) {
           slot = EDataSlot::TzDatetime;
        }
    }

    auto retType = builder.Primitive(GetDataTypeInfo(slot).TypeId);
    if (meta.IsOptional) {
        retType = builder.Optional()->Item(retType).Build();
    }

    return retType;
}

class TRunImpl : public TBoxedValue {
public:
    template<bool IsBlock>
    class TStreamValue : public TBoxedValue {
    public:
        TStreamValue(const IValueBuilder* valueBuilder, DB::QueryPipeline&& queryPipeline,
            const std::vector<ui32>& outBlockIndexInStruct,
            const std::vector<TColumnMeta>& outMeta,
            const std::shared_ptr<DB::Context>& sessionContext,
            const std::shared_ptr<DB::Context>& queryContext,
            const TSourcePosition& pos,
            ui32 tzId)
            : ValueBuilder(valueBuilder)
            , QueryPipeline(std::move(queryPipeline))
            , Executor(QueryPipeline)
            , OutBlockIndexInStruct(outBlockIndexInStruct)
            , OutMeta(outMeta)
            , SessionContext(sessionContext)
            , QueryContext(queryContext)
            , Pos(pos)
            , TzId(tzId)
        {
            // only keep ownership
            Y_UNUSED(SessionContext);
            Y_UNUSED(QueryContext);
        }

        EFetchStatus Fetch(TUnboxedValue& result) override {
            try {
                if (IsFinished) {
                    return EFetchStatus::Finish;
                }

                for (;;) {
                    if (IsBlock || CurrentRow >= CurrentChunk.getNumRows()) {
                        if (!Executor.pull(CurrentChunk)) {
                            IsFinished = true;
                            return EFetchStatus::Finish;
                        }

                        CurrentRow = 0;
                    }

                    TUnboxedValue* items = nullptr;
                    TUnboxedValue row = ValueBuilder->NewArray(OutBlockIndexInStruct.size(), items);
                    for (ui32 i = 0; i < OutBlockIndexInStruct.size(); ++i) {
                        ui32 structPos = OutBlockIndexInStruct[i];
                        const auto& meta = OutMeta[structPos];
                        auto output = CurrentChunk.getColumns()[i];
                        while (auto constOutput = DB::checkAndGetColumn<DB::ColumnConst>(output.get())) {
                            output = constOutput->getDataColumnPtr();
                        }

                        Y_ENSURE(output->size() == CurrentChunk.getNumRows());
                        items[structPos] = ConvertOutputValue(output.get(), meta, TzId, ValueBuilder, CurrentRow);
                    }

                    result = std::move(row);
                    if constexpr (!IsBlock) {
                        ++CurrentRow;
                    }
                    return EFetchStatus::Ok;
                }
            }
            catch (const DBPoco::Exception& e) {
                UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).c_str());
            }
            catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.what()).c_str());
            }
        }

    private:
        const IValueBuilder* ValueBuilder;
        DB::QueryPipeline QueryPipeline;
        DB::PullingPipelineExecutor Executor;
        const std::vector<ui32> OutBlockIndexInStruct; // block order -> struct order
        const std::vector<TColumnMeta> OutMeta; // in struct order
        const std::shared_ptr<DB::Context> SessionContext;
        const std::shared_ptr<DB::Context> QueryContext;
        const TSourcePosition Pos;
        const ui32 TzId;
        DB::Chunk CurrentChunk;
        size_t CurrentRow = 0;
        bool IsFinished = false;
    };

    TRunImpl(const std::string& query,
        const std::shared_ptr<DB::Context>& sessionContext,
        const TSourcePosition& pos, const TType* outputRowType,
        const std::unordered_map<TString, TColumnMeta>& metaForColumns,
        const ITypeInfoHelper::TPtr& typeHelper, const DB::Block& header, bool withInput, ui32 tzIndex)
        : Query(query)
        , SessionContext(sessionContext)
        , Pos(pos)
        , OutputTypeInspector(*typeHelper, outputRowType)
        , WithInput(withInput)
        , TzIndex(tzIndex)
    {
        Y_ENSURE(OutputTypeInspector);
        Y_ENSURE(OutputTypeInspector.GetMembersCount() == header.getNames().size());
        std::unordered_map<std::string, ui32> structIndicies;
        for (ui32 i = 0; i < OutputTypeInspector.GetMembersCount(); ++i) {
            auto name = OutputTypeInspector.GetMemberName(i);
            OutMeta.push_back(metaForColumns.find(TString(name))->second);
            structIndicies[std::string(name.Data(), name.Size())] = i;
        }

        OutBlockIndexInStruct.reserve(header.getNames().size());
        for (auto x : header.getNames()) {
            Y_ENSURE(structIndicies.count(x));
            OutBlockIndexInStruct.push_back(structIndicies[x]);
        }
    }

    TUnboxedValue Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const final
    {
        try {
            auto queryContext = DB::Context::createCopy(SessionContext);
            auto sessionHostCtx = GetHostContext(SessionContext);
            auto queryHostCtx = std::make_shared<THostContext>(*sessionHostCtx);
            if (WithInput) {
                queryHostCtx->InputValue = args[0];
                queryHostCtx->ValueBuilder = valueBuilder;
            }

            ui32 tzId = 0;
            if (!WithInput && TzIndex) {
                auto tz = args[TzIndex];
                auto tzRef = tz.AsStringRef();
                if (!valueBuilder->GetDateBuilder().FindTimezoneId(tzRef, tzId)) {
                    tzId = 0xffffu;
                }
            }

            queryContext->getHostContext() = queryHostCtx;
            queryContext->makeQueryContext();
            auto io = executeQuery(Query, queryContext, DB::QueryFlags{.internal=true}, DB::QueryProcessingStage::Complete);
            if (!io.second.pipeline.initialized()) {
                throw yexception() << "Pipeline is not initialized";
            }

            // TODO: IsBlock = true
            return TUnboxedValuePod(new TStreamValue<false>(valueBuilder, std::move(io.second.pipeline),
                OutBlockIndexInStruct, OutMeta, SessionContext, queryContext, Pos, tzId));
        }
        catch (const DBPoco::Exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).c_str());
        }
        catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.what()).c_str());
        }
    }

private:
    std::string Query;
    DB::ContextMutablePtr SessionContext;
    const TSourcePosition Pos;
    TStructTypeInspector OutputTypeInspector;
    const bool WithInput;
    const ui32 TzIndex;
    std::vector<ui32> OutBlockIndexInStruct; // block order -> struct order
    std::vector<TColumnMeta> OutMeta; // in struct order
};

class TRemoteSourceImpl : public TBoxedValue {
    template<bool IsBlock>
    class TStreamValue : public TBoxedValue, public INotify {
    public:
        TStreamValue(
            TCHContext& chCtx,
            const IValueBuilder* valueBuilder,
            const std::shared_ptr<DB::RemoteQueryExecutor>& remoteQuery,
            const DB::ContextPtr & sessionContext,
            const DB::ContextPtr & queryContext,
            const TSourcePosition& pos,
            ui32 tzId,
            const std::vector<ui32>& outBlockIndexInStruct,
            const std::vector<TColumnMeta>& outMeta,
            bool async,
            const TUnboxedValue& callback
        )
            : ChCtx(chCtx)
            , ValueBuilder(valueBuilder)
            , RemoteQuery(remoteQuery)
            , SessionContext(DB::Context::createCopy(sessionContext))
            , QueryContext(DB::Context::createCopy(queryContext))
            , Pos(pos)
            , TzId(tzId)
            , OutBlockIndexInStruct(outBlockIndexInStruct)
            , OutMeta(outMeta)
            , Async(async)
            , NotifyKey(async ? chCtx.AllocateNotifyKey() : 0)
            , Callback(callback)
        {
            // only keep ownership
            Y_UNUSED(SessionContext);
            Y_UNUSED(QueryContext);
        }

        ~TStreamValue()
        {
            if (Async) {
               ChCtx.RemoveNotify(NotifyKey);
            }
        }

        void Do() override {
            Callback.Run(nullptr, nullptr);
        }

        EFetchStatus Fetch(TUnboxedValue& result) override {
            try {
                if (IsFinished) {
                    return EFetchStatus::Finish;
                }

                for (;;) {
                    if (IsBlock || CurrentRow >= CurrentBlock.rows()) {
                        if (Async) {
                            auto res = RemoteQuery->readAsync();
                            if (res.getType() == DB::RemoteQueryExecutor::ReadResult::Type::FileDescriptor) {
                                auto fd = res.getFileDescriptor();
                                ChCtx.Wait(NotifyKey, this, fd);
                                return EFetchStatus::Yield;
                            }

                            CurrentBlock = res.getBlock();
                        } else {
                            CurrentBlock = RemoteQuery->readBlock();
                        }

                        if (!CurrentBlock.rows()) {
                            IsFinished = true;
                            return EFetchStatus::Finish;
                        }

                        CurrentRow = 0;
                    }

                    TUnboxedValue* items = nullptr;
                    TUnboxedValue row = ValueBuilder->NewArray(OutBlockIndexInStruct.size(), items);
                    for (ui32 i = 0; i < OutBlockIndexInStruct.size(); ++i) {
                        ui32 structPos = OutBlockIndexInStruct[i];
                        const auto& meta = OutMeta[structPos];
                        items[structPos] = ConvertOutputValue(CurrentBlock.getByPosition(i).column.get(), meta, TzId,
                            ValueBuilder, CurrentRow);
                    }

                    result = std::move(row);
                    if constexpr (!IsBlock) {
                        ++CurrentRow;
                    }
                    return EFetchStatus::Ok;
                }
            } catch (const DBPoco::Exception& e) {
                UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).c_str());
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << ValueBuilder->WithCalleePosition(Pos) << " " << e.what()).c_str());
            }
        }

    private:
        TCHContext& ChCtx;
        const IValueBuilder* ValueBuilder;
        std::shared_ptr<DB::RemoteQueryExecutor> RemoteQuery;
        std::shared_ptr<DB::Context> SessionContext;
        std::shared_ptr<DB::Context> QueryContext;
        const TSourcePosition Pos;
        const ui32 TzId;
        const std::vector<ui32> OutBlockIndexInStruct;
        const std::vector<TColumnMeta> OutMeta;
        const bool Async;
        const ui64 NotifyKey;
        const TUnboxedValue Callback;

        DB::Block CurrentBlock;
        size_t CurrentRow = 0;
        bool IsFinished = false;
        std::unique_ptr<DB::RemoteQueryExecutorReadContext> ReadContext;
    };

public:
    TRemoteSourceImpl(
        TCHContext& chCtx,
        const std::shared_ptr<DB::Context>& sessionContext,
        const DB::ConnectionPoolWithFailoverPtr& pool,
        const TString& query,
        const DB::Block& header,
        const TSourcePosition& pos, const TType* itemType,
        const ITypeInfoHelper::TPtr& typeHelper,
        const std::unordered_map<TString, TColumnMeta>& metaForColumns,
        bool async)
        : ChCtx(chCtx)
        , SessionContext(sessionContext)
        , Pool(pool)
        , Query(query)
        , Header(header)
        , Pos(pos)
        , OutputTypeInspector(*typeHelper, itemType)
        , Async(async)
    {
        Y_ENSURE(OutputTypeInspector);
        Y_ENSURE(OutputTypeInspector.GetMembersCount() == header.getNames().size());
        std::unordered_map<std::string, ui32> structIndicies;
        for (ui32 i = 0; i < OutputTypeInspector.GetMembersCount(); ++i) {
            auto name = OutputTypeInspector.GetMemberName(i);
            OutMeta.push_back(metaForColumns.find(TString(name))->second);
            structIndicies[std::string(name.Data(), name.Size())] = i;
        }

        OutBlockIndexInStruct.reserve(header.getNames().size());
        for (auto x : header.getNames()) {
            Y_ENSURE(structIndicies.count(x));
            OutBlockIndexInStruct.push_back(structIndicies[x]);
        }
    }

    TUnboxedValue Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const final
    {
        try {
            ui32 tzId = 0;
            auto tz = args[0];
            auto tzRef = tz.AsStringRef();
            if (!valueBuilder->GetDateBuilder().FindTimezoneId(tzRef, tzId)) {
                tzId = 0xffffu;
            }

            TUnboxedValue callback;
            if (Async) {
                callback = args[1];
            }

            auto queryContext = DB::Context::createCopy(SessionContext);
            queryContext->makeQueryContext();
            auto remoteQuery = std::make_shared<DB::RemoteQueryExecutor>(Pool, Query, Header, queryContext);
            remoteQuery->setPoolMode(DB::PoolMode::GET_ONE);
            // TODO: IsBlock = true;
            return TUnboxedValuePod(new TStreamValue<false>(ChCtx, valueBuilder, remoteQuery, SessionContext, queryContext, Pos, tzId,
                OutBlockIndexInStruct, OutMeta, Async, callback));
        }
        catch (const DBPoco::Exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.displayText()).c_str());
        }
        catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << valueBuilder->WithCalleePosition(Pos) << " " << e.what()).c_str());
        }
    }

private:
    TCHContext& ChCtx;
    DB::ContextPtr SessionContext;
    const DB::ConnectionPoolWithFailoverPtr Pool;
    TString Query;
    DB::Block Header;
    const TSourcePosition Pos;
    TStructTypeInspector OutputTypeInspector;
    const bool Async;
    std::vector<ui32> OutBlockIndexInStruct; // block order -> struct order
    std::vector<TColumnMeta> OutMeta; // in struct order
};

template <typename T>
TString MakeEnumImpl(const T& values) {
    TStringBuilder str;
    str << "Enum<";
    bool first = true;
    for (const auto& value : values) {
        if (!first) {
            str << ',';
        }
        else {
            first = false;
        }

        str << "'" << value.first << "'";
    }

    str << ">";
    return str;
}

std::optional<TString> MakeYqlType(DB::DataTypePtr type, bool validTz) {
    if (type->getTypeId() == DB::TypeIndex::Enum8) {
        const DB::DataTypeEnum8* enum8 = DB::checkAndGetDataType<DB::DataTypeEnum8>(type.get());
        return MakeEnumImpl(enum8->getValues());
    }

    if (type->getTypeId() == DB::TypeIndex::Enum16) {
        const DB::DataTypeEnum16* enum16 = DB::checkAndGetDataType<DB::DataTypeEnum16>(type.get());
        return MakeEnumImpl(enum16->getValues());
    }

    if (type->getTypeId() == DB::TypeIndex::AggregateFunction) {
        return "Tagged<String,'" + TString(type->getName()) + "'>";
    }

    if (type->getTypeId() == DB::TypeIndex::Array) {
        const DB::DataTypeArray* array = DB::checkAndGetDataType<DB::DataTypeArray>(type.get());
        type = array->getNestedType();
        if (type->getTypeId() == DB::TypeIndex::Nothing) {
            return "EmptyList";
        }

        auto inner = MakeYqlType(type, validTz);
        if (!inner) {
            return std::nullopt;
        }

        return "List<" + *inner + ">";
    }

    if (type->getTypeId() == DB::TypeIndex::Tuple) {
        const DB::DataTypeTuple* tuple = DB::checkAndGetDataType<DB::DataTypeTuple>(type.get());
        const auto& elems = tuple->getElements();
        TStringBuilder str;
        str << "Tuple<";
        bool first = true;
        for (const auto& e : elems) {
            auto inner = MakeYqlType(e, validTz);
            if (!inner) {
                return std::nullopt;
            }

            if (!first) {
                str << ',';
            } else {
                first = false;
            }

            str << *inner;
        }

        str << ">";
        return str;
    }

    if (type->isNullable()) {
       type = removeNullable(type);
       if (type->getTypeId() == DB::TypeIndex::Nothing) {
          return "Null";
       }

       auto inner = MakeYqlType(type, validTz);
       if (!inner) {
           return std::nullopt;
       }

       return "Optional<" + *inner + ">";
   }

   if (type->getTypeId() == DB::TypeIndex::UInt8) return "Uint8";
   else if (type->getTypeId() == DB::TypeIndex::Int8) return "Int8";
   else if (type->getTypeId() == DB::TypeIndex::UInt16) return "Uint16";
   else if (type->getTypeId() == DB::TypeIndex::Int16) return "Int16";
   else if (type->getTypeId() == DB::TypeIndex::UInt32) return "Uint32";
   else if (type->getTypeId() == DB::TypeIndex::Int32) return "Int32";
   else if (type->getTypeId() == DB::TypeIndex::UInt64) return "Uint64";
   else if (type->getTypeId() == DB::TypeIndex::Int64) return "Int64";
   else if (type->getTypeId() == DB::TypeIndex::Float32) return "Float";
   else if (type->getTypeId() == DB::TypeIndex::Float64) return "Double";
   else if (type->getTypeId() == DB::TypeIndex::String) return "String";
   else if (type->getTypeId() == DB::TypeIndex::FixedString) return "String";
   else if (validTz && type->getTypeId() == DB::TypeIndex::Date) return "TzDate";
   else if (validTz && type->getTypeId() == DB::TypeIndex::DateTime) return "TzDatetime";
   else if (type->getTypeId() == DB::TypeIndex::UUID) return "Uuid";
   else return std::nullopt;
}

SIMPLE_UDF(TToYqlType, TOptional<char*>(char*, char*)) {
    auto ref = args[0].AsStringRef();
    auto tzRef = args[1].AsStringRef();
    ui32 tzId;
    bool validTz = valueBuilder->GetDateBuilder().FindTimezoneId(tzRef, tzId);
    DB::String typeStr(ref.Data(), ref.Data() + ref.Size());
    auto type = DB::DataTypeFactory::instance().get(typeStr);
    auto ret = MakeYqlType(type, validTz);
    if (!ret) {
        return TUnboxedValue();
    }

    return valueBuilder->NewString(*ret);
}

template<bool MaybeOptional = true>
bool GetDataType(const ITypeInfoHelper& typeHelper, const TType* type, TColumnMeta& meta) {
    switch (typeHelper.GetTypeKind(type)) {
        case ETypeKind::Tuple: {
            meta.IsTuple = true;
            const TTupleTypeInspector tupleType(typeHelper, type);
            meta.Items.resize(tupleType.GetElementsCount());
            for (auto i = 0U; i < meta.Items.size(); ++i)
                if (!GetDataType(typeHelper, tupleType.GetElementType(i), meta.Items[i]))
                    return false;
            return true;
        }
        case ETypeKind::List:
            meta.IsList = true;
            type = TListTypeInspector(typeHelper, type).GetItemType();
            meta.Items.resize(1U);
            return GetDataType(typeHelper, type, meta.Items.front());
        case ETypeKind::Optional:
            if constexpr (MaybeOptional) {
                meta.IsOptional = true;
                type = TOptionalTypeInspector(typeHelper, type).GetItemType();
                return GetDataType<false>(typeHelper, type, meta);
            } else
                break;
        case ETypeKind::Data: {
            const TDataAndDecimalTypeInspector dataType(typeHelper, type);
            meta.Slot = GetDataSlot(dataType.GetTypeId());
            meta.Precision = dataType.GetPrecision();
            meta.Scale = dataType.GetScale();
            return true;
        }
        default:
            break;
    }
    return false;
}

} // namespace

class TClickHouseModule : public IUdfModule
{
public:
    TClickHouseModule() = default;

    static const TStringRef& Name() {
        static const auto name = TStringRef::Of("ClickHouse");
        return name;
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(IFunctionsSink& sink) const final {
        LazyInitContext();
        const auto & factory = DB::FunctionFactory::instance();
        for (const auto& name : factory.getAllRegisteredNames()) {
            if (name != "run" && name != "source" && name != "remoteSource" && name != "remoteSourceAsync") {
                sink.Add(name)->SetTypeAwareness();
            }
        }

        sink.Add(TStringRef::Of("run"))->SetTypeAwareness();
        sink.Add(TStringRef::Of("source"))->SetTypeAwareness();
        sink.Add(TStringRef::Of("remoteSource"))->SetTypeAwareness();
        sink.Add(TStringRef::Of("remoteSourceAsync"))->SetTypeAwareness();
    }

    void BuildFunctionTypeInfo(
                        const TStringRef& name,
                        TType* userType,
                        const TStringRef& typeConfig,
                        ui32 flags,
                        IFunctionTypeInfoBuilder& builder) const final
    {
        try {
            LazyInitContext();
            auto argBuilder = builder.Args();


            if (!userType) {
                builder.SetError("Missing user type");
                return;
            }

            const auto typeHelper = builder.TypeInfoHelper();
            const auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
                builder.SetError("Missing or invalid user type.");
                return;
            }

            const auto argsTypeTuple = userTypeInspector.GetElementType(0);
            const auto argsTypeInspector = TTupleTypeInspector(*typeHelper, argsTypeTuple);
            if (!argsTypeInspector) {
                builder.SetError("Invalid user type - expected tuple.");
                return;
            }

            ui32 argsCount = argsTypeInspector.GetElementsCount();
            if (name == "remoteSource" || name == "remoteSourceAsync") {
                std::string db, table, sql;
                std::unordered_map<TString, TColumnMeta> metaForColumns;
                std::unordered_set<TString> columns;

                DB::Block headerBlock;
                const DB::DataTypeFactory & dataTypeFactory = DB::DataTypeFactory::instance();

                const std::string str(typeConfig);
                const JSON json(str);
                const bool secure = json["secure"].getBool();
                const std::string host = json["host"].getString();
                const ui16 port = json["port"].getUInt();
                const std::string tokenAlias = json["token"].getString();

                if (json.has("query")) {
                    sql = json["query"].getString();
                    for (const auto& x : json["schema"]) {
                        auto name = x["name"].getString();
                        auto type = x["type"].getString();
                        DB::ColumnWithTypeAndName elem;
                        elem.name = name;
                        elem.type = dataTypeFactory.get(type);
                        headerBlock.insert(elem);

                        TColumnMeta meta = MetaFromClickHouse(elem.type, true);
                        metaForColumns[TString(elem.name)] = meta;
                    }
                } else {
                    db = json["db"].getString();
                    table = json["table"].getString();
                    for (const auto& col : json["columns"]) {
                        columns.emplace(col.getString());
                    }
                }

                TStringRef tokenKey = (TStringBuf)tokenAlias;
                if (!builder.GetSecureParam(tokenKey, tokenKey)) {
                    throw yexception() << "Unable to get token value: " << tokenAlias;
                }

                std::vector<TString> parts;
                StringSplitter((TStringBuf)tokenKey).Split('#').AddTo(&parts);
                if (parts.size() != 3 || parts[0] != "basic") {
                    throw yexception() << "Unexpected content of token value: " << tokenAlias;
                }

                auto pool = std::make_shared<DB::ConnectionPool>(
                    1 /*max_connections*/ ,
                    host,
                    port,
                    "" /*default_database*/,
                    parts[1] /*user*/,
                    parts[2] /*password*/,
                    "notchunked" /*proto_send_chunked*/,
                    "notchunked" /*proto_recv_chunked*/,
                    "" /*quota_key*/,
                    "" /*cluster*/,
                    "" /*cluster_secret*/,
                    "server" /*client_name*/,
                    DB::Protocol::Compression::Enable,
                    secure ? DB::Protocol::Secure::Enable : DB::Protocol::Secure::Disable);

                auto failPool = std::make_shared<DB::ConnectionPoolWithFailover>(
                    DB::ConnectionPoolPtrs{ pool }, DB::LoadBalancing::RANDOM);

                auto sessionContext = DB::Context::createCopy(Ctx->Context);
                sessionContext->makeSessionContext();

                auto structBuilder = builder.Struct();
                if (!sql.empty()) {
                    for (const auto& x : metaForColumns) {
                        structBuilder->AddField(x.first, MakeType(x.second, builder, true), nullptr);
                    }
                } else {
                    TString fullTable = TString(DB::backQuoteIfNeed(std::string(db))) +
                        "." + TString(DB::backQuoteIfNeed(std::string(table)));

                    TString query = "DESC TABLE " + fullTable;

                    DB::Block sampleBlock
                    {
                        { DB::ColumnString::create(), std::make_shared<DB::DataTypeString>(), "name" },
                        { DB::ColumnString::create(), std::make_shared<DB::DataTypeString>(), "type" },
                    };

                    auto queryContext = DB::Context::createCopy(sessionContext);
                    queryContext->makeQueryContext();
                    auto remoteQuery = std::make_shared<DB::RemoteQueryExecutor>(failPool, query, sampleBlock, queryContext);
                    remoteQuery->setPoolMode(DB::PoolMode::GET_ONE);

                    TStringStream tableQuery;
                    tableQuery << "SELECT ";
                    bool firstColumn = true;
                    while (DB::Block current = remoteQuery->readBlock())
                    {
                        DB::ColumnPtr name = current.getByName("name").column;
                        DB::ColumnPtr type = current.getByName("type").column;
                        size_t size = name->size();

                        for (size_t i = 0; i < size; ++i) {
                            DB::ColumnWithTypeAndName elem;
                            elem.name = (*name)[i].safeGet<String>();
                            if (!columns.contains(TString(elem.name))) {
                                continue;
                            }

                            String dataTypeName = (*type)[i].safeGet<String>();
                            elem.type = dataTypeFactory.get(dataTypeName);
                            headerBlock.insert(elem);

                            TColumnMeta meta = MetaFromClickHouse(elem.type, true);
                            metaForColumns.emplace(elem.name, meta);
                            structBuilder->AddField(TString(elem.name), MakeType(meta, builder, true), nullptr);
                            if (firstColumn) {
                                firstColumn = false;
                            } else {
                                tableQuery << ",";
                            }

                            tableQuery << TString(DB::backQuoteIfNeed(elem.name));
                        }
                    }

                    tableQuery << " FROM " << fullTable;
                    sql = tableQuery.Str();
                }

                auto retRowType = structBuilder->Build();
                if (!(flags & TFlags::TypesOnly)) {
                    Ctx->InitPollingThread();
                    builder.Implementation(new TRemoteSourceImpl(*Ctx, sessionContext, failPool, std::string(sql), headerBlock,
                        builder.GetSourcePosition(), retRowType, typeHelper, metaForColumns, name == "remoteSourceAsync"));
                }

                auto retType = builder.Stream()->Item(retRowType).Build();
                argBuilder->Add<char*>();
                if (name == "remoteSourceAsync") {
                    auto callableBuilder = builder.Callable(0);
                    callableBuilder->Returns(builder.Void());
                    auto callbackType = callableBuilder->Build();
                    argBuilder->Add(callbackType);
                }

                argBuilder->Done();
                builder.Returns(retType);
                return;
            }

            NYT::TNode settings = NYT::NodeFromYsonString(typeConfig);
            Y_ENSURE(settings.IsMap());
            auto args = settings["args"];
            Y_ENSURE(args.IsMap());

            if (name == "run" || name == "source") {
                if (name == "run" && argsCount != 2) {
                    builder.SetError("run function expected 2 arguments");
                    return;
                }

                if (name == "source" && !(argsCount >= 1 && argsCount <= 2)) {
                    builder.SetError("source function expected 1 or 2 arguments");
                    return;
                }

                auto sessionContext = DB::Context::createCopy(Ctx->Context);
                sessionContext->makeSessionContext();
                auto queryIndex = (name == "run") ? "1" : "0";
                auto tzIndex = (name == "source") ? 1u : 4u;
                auto tzIndexStr = ToString(tzIndex);
                const TType* itemType = nullptr;
                if (name == "run") {
                    auto arg0 = argsTypeInspector.GetElementType(0);
                    if (typeHelper->GetTypeKind(arg0) == ETypeKind::List) {
                        itemType = TListTypeInspector(*typeHelper, arg0).GetItemType();
                    }
                    else if (typeHelper->GetTypeKind(arg0) == ETypeKind::Stream) {
                        itemType = TStreamTypeInspector(*typeHelper, arg0).GetItemType();
                    }
                    else {
                        throw yexception() << "Unsupported first argument input type: " << FormatType(arg0, typeHelper);
                    }

                    if (typeHelper->GetTypeKind(itemType) != ETypeKind::Struct) {
                        throw yexception() << "Unsupported first argument input row type: " << FormatType(itemType, typeHelper);
                    }

                    sessionContext->getHostContext() = std::make_shared<THostContext>(itemType, typeHelper);
                } else {
                    sessionContext->getHostContext() = std::make_shared<THostContext>(nullptr, typeHelper);
                }

                if (!args.HasKey(queryIndex) || args[queryIndex]["type"].AsString() != "String") {
                    throw yexception() << "Query must be a literal string";
                }

                auto argQuery = TString(args[queryIndex]["value"].AsString());
                auto argStr = std::string(argQuery.data(), argQuery.size());
                auto queryContext = DB::Context::createCopy(sessionContext);
                queryContext->makeQueryContext();
                auto io = executeQuery(argStr, queryContext, DB::QueryFlags{.internal=true}, DB::QueryProcessingStage::Complete);

                if (!io.second.pipeline.initialized()) {
                    throw yexception() << "Pipeline is not initialized";
                }

                if (!io.second.pipeline.pulling()) {
                    throw yexception() << "INSERT is not allowed";
                }

                const auto& outHeader = io.second.pipeline.getHeader();
                auto structBuilder = builder.Struct();
                std::unordered_map<TString, TColumnMeta> metaForColumns;
                for (const auto& x : outHeader.getNamesAndTypesList()) {
                    auto meta = MetaFromClickHouse(x.type, tzIndex < argsCount);
                    structBuilder->AddField(x.name, MakeType(meta, builder, tzIndex < argsCount), nullptr);
                    metaForColumns.emplace(x.name, meta);
                }

                auto retRowType = structBuilder->Build();
                if (!(flags & TFlags::TypesOnly)) {
                    builder.Implementation(new TRunImpl(argStr, sessionContext, builder.GetSourcePosition(),
                        retRowType, metaForColumns, typeHelper, outHeader, name == "run", tzIndex < argsCount ? tzIndex : 0));
                }

                auto retType = builder.Stream()->Item(retRowType).Build();
                if (name == "run") {
                    auto streamInType = builder.Stream()->Item(itemType).Build();
                    argBuilder->Add(streamInType).Flags(ICallablePayload::TArgumentFlags::NoYield);
                }

                argBuilder->Add<char*>();
                if (name == "source" && (tzIndex < argsCount)) {
                    argBuilder->Add<char*>();
                }

                argBuilder->Done();
                builder.UserType(userType);
                builder.Returns(retType);
                return;
            }

            const auto & factory = DB::FunctionFactory::instance();
            DB::FunctionOverloadResolverPtr funcBuilder = factory.tryGet(std::string(name.Data(), name.Size()), Ctx->Context);
            if (!funcBuilder) {
                builder.SetError(TStringBuilder() << "Unknown function: " << TStringBuf(name));
                return;
            }

            if (funcBuilder->isStateful()) {
                builder.SetError("Stateful function is not supported");
                return;
            }

            funcBuilder->checkNumberOfArguments(argsCount);
            DB::ColumnsWithTypeAndName arguments;
            std::vector<TColumnMeta> argsMeta;
            std::optional<bool> isScalarBlock;
            for (ui32 i = 0; i < argsCount; ++i) {
                DB::ColumnWithTypeAndName col;
                auto argType = argsTypeInspector.GetElementType(i);
                argBuilder->Add(argType);

                try {
                    TColumnMeta meta = MakeMeta(argType, typeHelper);
                    if (meta.IsScalarBlock) {
                        arrow::MemoryPool *pool = arrow::default_memory_pool();
                        meta.Reader = IDBColumnReader::Make(*typeHelper, meta, *pool);
                        meta.Writer = IDBColumnWriter::Make(meta);
                        if (!isScalarBlock) {
                            isScalarBlock = meta.IsScalarBlock;
                        } else {
                            isScalarBlock = *isScalarBlock && *meta.IsScalarBlock;
                        }
                    }

                    argsMeta.push_back(meta);
                    col.name = TStringBuilder() << "arg" << i;
                    col.type = MetaToClickHouse(meta);
                    auto argValue = args[ToString(i)];
                    if (!argValue.IsUndefined()) {
                        // make const column
                        auto type = argValue["type"].AsString();
                        auto value = argValue["value"].AsString();
                        col.column = MakeConstColumn(type, value);
                    }

                    arguments.push_back(col);
                } catch (yexception& e) {
                    builder.SetError(TStringBuilder() << e.what() << " at arg " << (i + 1));
                    return;
                }
            }

            auto func = funcBuilder->build(arguments);
            builder.UserType(userType);
            argBuilder->Done();
            auto funcRet = func->getResultType();
            TColumnMeta retMeta;
            try {
                retMeta = MetaFromClickHouse(funcRet, false);
            } catch (yexception& e) {
                builder.SetError(TStringBuf(e.what()));
                return;
            }

            TType* retType = MakeType(retMeta, builder, false);
            if (isScalarBlock) {
                FillArrowType(retMeta, retType, typeHelper);
                retMeta.IsScalarBlock = isScalarBlock;
                arrow::MemoryPool *pool = arrow::default_memory_pool();
                retMeta.Reader = IDBColumnReader::Make(*typeHelper, retMeta, *pool);
                retMeta.Writer = IDBColumnWriter::Make(retMeta);
                retType = builder.Block(*isScalarBlock)->Item(retType).Build();
            }

            builder.Returns(retType);
            builder.SupportsBlocks();
            if (!(flags & TFlags::TypesOnly)) {
                builder.Implementation(new TImpl(func, arguments, retMeta, argsMeta,
                    builder.GetSourcePosition()));
            }
        }
        catch (const DBPoco::Exception& e) {
            builder.SetError(e.displayText());
        }
        catch (const std::exception& e) {
            builder.SetError(TStringBuf(e.what()));
        }
    }
private:
    void LazyInitContext() const {
        const std::unique_lock lock(CtxMutex);
        if (!Ctx) {
            if (auto ctx = StaticCtx.lock()) {
                Ctx = std::move(ctx);
            } else {
                StaticCtx = Ctx = std::make_shared<TCHContext>();
            }
        }
    }

    static std::mutex CtxMutex;
    static TCHContext::TWeakPtr StaticCtx;
    mutable TCHContext::TPtr Ctx;
};

std::mutex TClickHouseModule::CtxMutex;
TCHContext::TWeakPtr TClickHouseModule::StaticCtx;

REGISTER_MODULES(TClickHouseModule);
