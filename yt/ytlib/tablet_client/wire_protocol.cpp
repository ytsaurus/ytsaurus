#include "wire_protocol.h"
#include "private.h"

#include <yt/ytlib/table_client/chunk_meta.pb.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/schemaful_writer.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/bitmap.h>
#include <yt/core/misc/chunked_memory_pool.h>
#include <yt/core/misc/chunked_output_stream.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/serialize.h>

#include <yt/core/compression/codec.h>

#include <contrib/libs/protobuf/io/coded_stream.h>

namespace NYT {
namespace NTabletClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TWireProtocolWriterTag
{ };

struct TWireProtocolReaderTag
{ };

static const size_t ReaderBufferChunkSize = 4096;

static const size_t WriterInitialBufferCapacity = 1024;
static const size_t PreallocateBlockSize = 4096;

static_assert(sizeof(i64) == SerializationAlignment, "Wrong serialization alignment");
static_assert(sizeof(double) == SerializationAlignment, "Wrong serialization alignment");
static_assert(sizeof(TUnversionedValue) == 2 * sizeof(i64), "Wrong TUnversionedValue size");

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolWriter::TImpl
{
public:
    TImpl()
        : Stream_(TWireProtocolWriterTag())
    {
        EnsureCapacity(WriterInitialBufferCapacity);
    }

    size_t GetByteSize() const
    {
        return Stream_.GetSize();
    }

    void WriteCommand(EWireProtocolCommand command)
    {
        WriteInt64(static_cast<int>(command));
    }

    void WriteTableSchema(const TTableSchema& schema)
    {
        WriteMessage(ToProto<NTableClient::NProto::TTableSchemaExt>(schema));
    }

    void WriteMessage(const ::google::protobuf::MessageLite& message)
    {
        int size = message.ByteSize();
        WriteInt64(size);
        EnsureAlignedUpCapacity(size);
        YCHECK(message.SerializePartialToArray(Current_, size));
        Current_ += AlignUp(size);
    }

    void WriteSchemafulRow(
        TUnversionedRow row,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        if (row) {
            WriteRowValues<true>(TRange<TUnversionedValue>(row.Begin(), row.End()), idMapping);
        } else {
            WriteRowValues<true>(TRange<TUnversionedValue>(), idMapping);
        }
    }

    void WriteUnversionedRow(
        TUnversionedRow row,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        if (row) {
            WriteRowValues<false>(TRange<TUnversionedValue>(row.Begin(), row.End()), idMapping);
        } else {
            WriteRowValues<false>(TRange<TUnversionedValue>(), idMapping);
        }
    }

    void WriteUnversionedRow(
        const TRange<TUnversionedValue>& values,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        WriteRowValues<false>(values, idMapping);
    }

    void WriteUnversionedRowset(
        const TRange<TUnversionedRow>& rowset,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        WriteRowCount(rowset);
        for (auto row : rowset) {
            WriteUnversionedRow(row, idMapping);
        }
    }

    void WriteSchemafulRowset(
        const TRange<TUnversionedRow>& rowset,
        const TNameTableToSchemaIdMapping* idMapping = nullptr)
    {
        WriteRowCount(rowset);
        for (auto row : rowset) {
            WriteSchemafulRow(row, idMapping);
        }
    }

    std::vector<TSharedRef> Finish()
    {
        FlushPreallocated();
        return Stream_.Flush();
    }

private:
    TChunkedOutputStream Stream_;
    char* BeginPreallocated_ = nullptr;
    char* EndPreallocated_ = nullptr;
    char* Current_ = nullptr;

    std::vector<TUnversionedValue> PooledValues_;


    void FlushPreallocated()
    {
        if (!Current_)
            return;

        YCHECK(Current_ <= EndPreallocated_);
        Stream_.Advance(Current_ - BeginPreallocated_);
        BeginPreallocated_ = EndPreallocated_ = Current_ = nullptr;
    }

    void EnsureCapacity(size_t more)
    {
        if (Y_LIKELY(Current_ + more < EndPreallocated_))
            return;

        FlushPreallocated();
        size_t size = std::max(PreallocateBlockSize, more);
        Current_ = BeginPreallocated_ = Stream_.Preallocate(size);
        EndPreallocated_ = BeginPreallocated_ + size;
    }

    void EnsureAlignedUpCapacity(size_t more)
    {
        EnsureCapacity(AlignUp(more));
    }

    void UnsafeWriteInt64(i64 value)
    {
        *reinterpret_cast<i64*>(Current_) = value;
        Current_ += sizeof(i64);
    }

    void WriteInt64(i64 value)
    {
        EnsureCapacity(sizeof(i64));
        UnsafeWriteInt64(value);
    }

    void UnsafeWriteRaw(const void* buffer, size_t size)
    {
        memcpy(Current_, buffer, size);
        Current_ += AlignUp(size);
    }

    void WriteRaw(const void* buffer, size_t size)
    {
        EnsureAlignedUpCapacity(size);
        UnsafeWriteRaw(buffer, size);
    }


    void WriteString(const Stroka& value)
    {
        WriteInt64(value.length());
        WriteRaw(value.begin(), value.length());
    }

    void WriteRowCount(const TRange<TUnversionedRow>& rowset)
    {
        int rowCount = static_cast<int>(rowset.Size());
        ValidateRowCount(rowCount);
        WriteInt64(rowCount);
    }

    template <bool Schemaful>
    void WriteRowValue(const TUnversionedValue& value)
    {
        // This includes the value itself and possible serialization alignment.
        i64 bytes = (Schemaful ? 1 : 2) * sizeof(i64);
        if (IsStringLikeType(value.Type)) {
            bytes += value.Length + (Schemaful ? sizeof(i64) : 0);
        }
        EnsureAlignedUpCapacity(bytes);

        const i64* rawValue = reinterpret_cast<const i64*>(&value);
        if (!Schemaful) {
            UnsafeWriteInt64(rawValue[0]);
        }
        switch (value.Type) {
            case EValueType::Int64:
            case EValueType::Uint64:
            case EValueType::Double:
            case EValueType::Boolean:
                UnsafeWriteInt64(rawValue[1]);
                break;

            case EValueType::String:
            case EValueType::Any:
                if (Schemaful) {
                    UnsafeWriteInt64(value.Length);
                }
                UnsafeWriteRaw(value.Data.String, value.Length);
                break;

            default:
                break;
        }
    }

    template <bool Schemaful>
    void WriteNullVector(const TRange<TUnversionedValue>& values)
    {
        if (Schemaful) {
            auto nullBitmap = TAppendOnlyBitmap<ui64>(values.Size());
            for (int index = 0; index < values.Size(); ++index) {
                nullBitmap.Append(values[index].Type == EValueType::Null);
            }
            WriteRaw(nullBitmap.Data(), nullBitmap.Size());
        }
    }

    template <bool Schemaful>
    void WriteRowValues(
        const TRange<TUnversionedValue>& values,
        const TNameTableToSchemaIdMapping* idMapping)
    {
        if (!values) {
            WriteInt64(-1);
            return;
        }

        int valueCount = values.Size();
        WriteInt64(valueCount);

        if (idMapping) {
            PooledValues_.resize(valueCount);
            for (int index = 0; index < valueCount; ++index) {
                const auto& srcValue = values[index];
                auto& dstValue = PooledValues_[index];
                dstValue = srcValue;
                dstValue.Id = (*idMapping)[srcValue.Id];
            }

            std::sort(
                PooledValues_.begin(),
                PooledValues_.end(),
                [](const TUnversionedValue& lhs, const TUnversionedValue& rhs) {
                    return lhs.Id < rhs.Id;
                });

            WriteNullVector<Schemaful>(
                TRange<TUnversionedValue>(PooledValues_.data(), PooledValues_.size()));
            for (int index = 0; index < valueCount; ++index) {
                WriteRowValue<Schemaful>(PooledValues_[index]);
            }
        } else {
            WriteNullVector<Schemaful>(values);
            for (const auto& value : values) {
                WriteRowValue<Schemaful>(value);
            }
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TWireProtocolWriter::TWireProtocolWriter()
    : Impl_(std::make_unique<TImpl>())
{ }

TWireProtocolWriter::~TWireProtocolWriter() = default;

size_t TWireProtocolWriter::GetByteSize() const
{
    return Impl_->GetByteSize();
}

std::vector<TSharedRef> TWireProtocolWriter::Finish()
{
    return Impl_->Finish();
}

void TWireProtocolWriter::WriteCommand(EWireProtocolCommand command)
{
    Impl_->WriteCommand(command);
}

void TWireProtocolWriter::WriteTableSchema(const TTableSchema& schema)
{
    Impl_->WriteTableSchema(schema);
}

void TWireProtocolWriter::WriteMessage(const ::google::protobuf::MessageLite& message)
{
    Impl_->WriteMessage(message);
}

void TWireProtocolWriter::WriteSchemafulRow(
    TUnversionedRow row,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteSchemafulRow(row, idMapping);
}

void TWireProtocolWriter::WriteUnversionedRow(
    TUnversionedRow row,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteUnversionedRow(row, idMapping);
}

void TWireProtocolWriter::WriteUnversionedRow(
    const TRange<TUnversionedValue>& row,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteUnversionedRow(row, idMapping);
}

void TWireProtocolWriter::WriteUnversionedRowset(
    const TRange<TUnversionedRow>& rowset,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteUnversionedRowset(rowset, idMapping);
}

void TWireProtocolWriter::WriteSchemafulRowset(
    const TRange<TUnversionedRow>& rowset,
    const TNameTableToSchemaIdMapping* idMapping)
{
    Impl_->WriteSchemafulRowset(rowset, idMapping);
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolReader::TImpl
{
public:
    explicit TImpl(
        const TSharedRef& data,
        TRowBufferPtr rowBuffer)
        : RowBuffer_(rowBuffer ? rowBuffer : New<TRowBuffer>(TWireProtocolReaderTag(), ReaderBufferChunkSize))
        , Data_(data)
        , Current_(Data_.Begin())
    { }

    const TRowBufferPtr& GetRowBuffer() const
    {
        return RowBuffer_;
    }

    bool IsFinished() const
    {
        return Current_ == Data_.End();
    }

    TIterator GetBegin() const
    {
        return Data_.Begin();
    }

    TIterator GetEnd() const
    {
        return Data_.End();
    }

    TIterator GetCurrent() const
    {
        return Current_;
    }

    void SetCurrent(TIterator current)
    {
        Current_ = current;
    }

    TSharedRef Slice(TIterator begin, TIterator end)
    {
        return Data_.Slice(begin, end);
    }

    EWireProtocolCommand ReadCommand()
    {
        return EWireProtocolCommand(ReadInt64());
    }

    TTableSchema ReadTableSchema()
    {
        NTableClient::NProto::TTableSchemaExt protoSchema;
        ReadMessage(&protoSchema);
        return FromProto<TTableSchema>(protoSchema);
    }

    void ReadMessage(::google::protobuf::MessageLite* message)
    {
        i64 size = ReadInt64();
        ::google::protobuf::io::CodedInputStream chunkStream(
            reinterpret_cast<const ui8*>(Current_),
            size);
        message->ParsePartialFromCodedStream(&chunkStream);
        Current_ += AlignUp(size);
    }

    TUnversionedRow ReadSchemafulRow(const TSchemaData& schemaData, bool deep)
    {
        return ReadRow<true>(&schemaData, deep);
    }

    TUnversionedRow ReadUnversionedRow(bool deep)
    {
        return ReadRow<false>(nullptr, deep);
    }

    TSharedRange<TUnversionedRow> ReadUnversionedRowset(bool deep)
    {
        return ReadRowset<false>(nullptr, deep);
    }

    TSharedRange<TUnversionedRow> ReadSchemafulRowset(const TSchemaData& schemaData, bool deep)
    {
        return ReadRowset<true>(&schemaData, deep);
    }

private:
    const TRowBufferPtr RowBuffer_;

    TSharedRef Data_;
    TIterator Current_;


    i64 ReadInt64()
    {
        YCHECK(Current_ + sizeof(i64) <= Data_.End());
        i64 result = *reinterpret_cast<const i64*>(Current_);
        Current_ += sizeof(result);
        return result;
    }

    i32 ReadInt32()
    {
        i64 result = ReadInt64();
        if (result > std::numeric_limits<i32>::max()) {
            THROW_ERROR_EXCEPTION("Value is too big to fit into int32");
        }
        return static_cast<i32>(result);
    }

    void Skip(size_t size)
    {
        YCHECK(Current_ + size <= Data_.End());
        Current_ += size;
        Current_ += GetPaddingSize(size);
    }

    const char* ReadRaw(size_t size)
    {
        YCHECK(Current_ + size <= Data_.End());
        auto result = Current_;
        Skip(size);
        return result;
    }


    template <bool Schemaful>
    void ReadRowValue(
        TUnversionedValue* value,
        const TSchemaData* schemaData,
        const TReadOnlyBitmap<ui64>& nullBitmap,
        bool deep,
        int index)
    {
        i64* rawValue = reinterpret_cast<i64*>(value);
        if (Schemaful) {
            rawValue[0] = (*schemaData)[index];
            if (nullBitmap[index]) {
                value->Type = EValueType::Null;
            }
        } else {
            rawValue[0] = ReadInt64();
        }

        switch (value->Type) {
            case EValueType::Int64:
            case EValueType::Uint64:
            case EValueType::Double:
            case EValueType::Boolean:
                rawValue[1] = ReadInt64();
                break;

            case EValueType::String:
            case EValueType::Any:
                if (Schemaful) {
                    value->Length = ReadInt32();
                }
                if (value->Length > MaxStringValueLength) {
                    THROW_ERROR_EXCEPTION("Value is too long: length %v, limit %v",
                        value->Length,
                        MaxStringValueLength);
                }
                value->Data.String = ReadRaw(value->Length);
                if (deep) {
                    *value = RowBuffer_->Capture(*value);
                }
                break;

            default:
                break;
        }
    }

    template <bool Schemaful>
    TUnversionedRow ReadRow(const TSchemaData* schemaData, bool deep)
    {
        int valueCount = ReadInt32();
        if (valueCount == -1) {
            return TUnversionedRow();
        }

        ValidateRowValueCount(valueCount);

        auto nullBitmap = TReadOnlyBitmap<ui64>();
        if (schemaData) {
            nullBitmap.Reset(reinterpret_cast<const ui64*>(Current_), valueCount);
            Skip(nullBitmap.GetByteSize());
        }

        auto row = RowBuffer_->Allocate(valueCount);
        for (int index = 0; index < valueCount; ++index) {
            ReadRowValue<Schemaful>(&row[index], schemaData, nullBitmap, deep, index);
        }
        return row;
    }

    template <bool Schemaful>
    TSharedRange<TUnversionedRow> ReadRowset(const TSchemaData* schemaData, bool deep)
    {
        int rowCount = ReadInt32();
        ValidateRowCount(rowCount);

        auto* rows = RowBuffer_->GetPool()->AllocateUninitialized<TUnversionedRow>(rowCount);
        for (int index = 0; index < rowCount; ++index) {
            rows[index] = ReadRow<Schemaful>(schemaData, deep);
        }

        return TSharedRange<TUnversionedRow>(rows, rows + rowCount, RowBuffer_);
    }
};

////////////////////////////////////////////////////////////////////////////////

TWireProtocolReader::TWireProtocolReader(
    const TSharedRef& data,
    TRowBufferPtr rowBuffer)
    : Impl_(std::make_unique<TImpl>(data, std::move(rowBuffer)))
{ }

TWireProtocolReader::~TWireProtocolReader() = default;

const TRowBufferPtr& TWireProtocolReader::GetRowBuffer() const
{
    return Impl_->GetRowBuffer();
}

bool TWireProtocolReader::IsFinished() const
{
    return Impl_->IsFinished();
}

auto TWireProtocolReader::GetBegin() const -> TIterator
{
    return Impl_->GetBegin();
}

auto TWireProtocolReader::GetEnd() const -> TIterator
{
    return Impl_->GetEnd();
}

auto TWireProtocolReader::GetCurrent() const -> TIterator
{
    return Impl_->GetCurrent();
}

void TWireProtocolReader::SetCurrent(TIterator current)
{
    Impl_->SetCurrent(current);
}

TSharedRef TWireProtocolReader::Slice(TIterator begin, TIterator end)
{
    return Impl_->Slice(begin, end);
}

EWireProtocolCommand TWireProtocolReader::ReadCommand()
{
    return Impl_->ReadCommand();
}

TTableSchema TWireProtocolReader::ReadTableSchema()
{
    return Impl_->ReadTableSchema();
}

void TWireProtocolReader::ReadMessage(::google::protobuf::MessageLite* message)
{
    Impl_->ReadMessage(message);
}

TUnversionedRow TWireProtocolReader::ReadUnversionedRow(bool deep)
{
    return Impl_->ReadUnversionedRow(deep);
}

TUnversionedRow TWireProtocolReader::ReadSchemafulRow(const TSchemaData& schemaData, bool deep)
{
    return Impl_->ReadSchemafulRow(schemaData, deep);
}

TSharedRange<TUnversionedRow> TWireProtocolReader::ReadUnversionedRowset(bool deep)
{
    return Impl_->ReadUnversionedRowset(deep);
}

TSharedRange<TUnversionedRow> TWireProtocolReader::ReadSchemafulRowset(const TSchemaData& schemaData, bool deep)
{
    return Impl_->ReadSchemafulRowset(schemaData, deep);
}

auto TWireProtocolReader::GetSchemaData(
    const TTableSchema& schema,
    const TColumnFilter& filter) -> TSchemaData
{
    TSchemaData schemaData;
    auto addColumn = [&](int id) {
        auto value = MakeUnversionedValueHeader(schema.Columns()[id].Type, id);
        schemaData.push_back(*reinterpret_cast<ui32*>(&value));
    };

    if (!filter.All) {
        for (int id : filter.Indexes) {
            addColumn(id);
        }
    } else {
        for (int id = 0; id < schema.Columns().size(); ++id) {
            addColumn(id);
        }
    }
    return schemaData;
}

auto TWireProtocolReader::GetSchemaData(const TTableSchema& schema) -> TSchemaData
{
    TSchemaData schemaData;
    for (int id = 0; id < schema.GetKeyColumnCount(); ++id) {
        TUnversionedValue value;
        value.Id = id;
        value.Type = schema.Columns()[id].Type;
        schemaData.push_back(*reinterpret_cast<ui32*>(&value));
    }
    return schemaData;
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolRowsetReader
    : public IWireProtocolRowsetReader
{
public:
    TWireProtocolRowsetReader(
        const std::vector<TSharedRef>& compressedBlocks,
        NCompression::ECodec codecId,
        const TTableSchema& schema,
        const NLogging::TLogger& logger)
        : CompressedBlocks_(compressedBlocks)
          , Codec_(NCompression::GetCodec(codecId))
          , Schema_(schema)
          , Logger(
            NLogging::TLogger(logger)
                .AddTag("ReaderId: %v", TGuid::Create()))
    {
        LOG_DEBUG("Wire protocol rowset reader created (BlockCount: %v, TotalCompressedSize: %v)",
            CompressedBlocks_.size(),
            GetByteSize(CompressedBlocks_));
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        if (Finished_) {
            return false;
        }

        if (BlockIndex_ >= CompressedBlocks_.size()) {
            Finished_ = true;
            LOG_DEBUG("Wire protocol rowset reader finished");
            return false;
        }

        const auto& compressedBlock = CompressedBlocks_[BlockIndex_];
        LOG_DEBUG("Started decompressing rowset reader block (BlockIndex: %v, CompressedSize: %v)",
            BlockIndex_,
            compressedBlock.Size());
        auto uncompressedBlock = Codec_->Decompress(compressedBlock);
        LOG_DEBUG("Finished decompressing rowset reader block (BlockIndex: %v, UncompressedSize: %v)",
            BlockIndex_,
            uncompressedBlock.Size());

        auto rowBuffer = New<TRowBuffer>(TWireProtocolReaderTag(), ReaderBufferChunkSize);
        WireReader_ = std::make_unique<TWireProtocolReader>(uncompressedBlock, std::move(rowBuffer));

        if (!SchemaChecked_) {
            auto actualSchema = WireReader_->ReadTableSchema();
            if (Schema_ != actualSchema) {
                THROW_ERROR_EXCEPTION("Schema mismatch while parsing wire protocol");
            }
            SchemaChecked_ = true;
        }

        rows->clear();
        while (!WireReader_->IsFinished()) {
            auto row = WireReader_->ReadUnversionedRow(false);
            rows->push_back(row);
        }
        ++BlockIndex_;

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

private:
    const std::vector<TSharedRef> CompressedBlocks_;
    NCompression::ICodec* const Codec_;
    const TTableSchema Schema_;
    const NLogging::TLogger Logger;

    int BlockIndex_ = 0;
    std::unique_ptr<TWireProtocolReader> WireReader_;
    bool Finished_ = false;
    bool SchemaChecked_ = false;

};

IWireProtocolRowsetReaderPtr CreateWireProtocolRowsetReader(
    const std::vector<TSharedRef>& compressedBlocks,
    NCompression::ECodec codecId,
    const TTableSchema& schema,
    const NLogging::TLogger& logger)
{
    return New<TWireProtocolRowsetReader>(
        compressedBlocks,
        codecId,
        schema,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

class TWireProtocolRowsetWriter
    : public IWireProtocolRowsetWriter
{
public:
    TWireProtocolRowsetWriter(
        NCompression::ECodec codecId,
        size_t desiredUncompressedBlockSize,
        const TTableSchema& schema,
        const NLogging::TLogger& logger)
        : Codec_(NCompression::GetCodec(codecId))
        , DesiredUncompressedBlockSize_(desiredUncompressedBlockSize)
        , Schema_(schema)
        , Logger(NLogging::TLogger(logger)
            .AddTag("WriterId: %v", TGuid::Create()))
    {
        LOG_DEBUG("Wire protocol rowset writer created (CodecId: %v, DesiredUncompressedBlockSize: %v)",
            codecId,
            DesiredUncompressedBlockSize_);
    }

    virtual TFuture<void> Close() override
    {
        if (!Closed_) {
            LOG_DEBUG("Wire protocol rowset writer closed");
            FlushBlock();
            Closed_ = true;
        }
        return VoidFuture;
    }

    virtual bool Write(const TRange<TUnversionedRow>& rows) override
    {
        YCHECK(!Closed_);
        for (auto row : rows) {
            if (!WireWriter_) {
                WireWriter_ = std::make_unique<TWireProtocolWriter>();
                if (!SchemaWritten_) {
                    WireWriter_->WriteTableSchema(Schema_);
                    SchemaWritten_ = true;
                }
            }
            WireWriter_->WriteUnversionedRow(row);
            if (WireWriter_->GetByteSize() >= DesiredUncompressedBlockSize_) {
                FlushBlock();
            }
        }
        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    virtual std::vector<TSharedRef> GetCompressedBlocks() override
    {
        YCHECK(Closed_);
        return CompressedBlocks_;
    }

private:
    NCompression::ICodec* const Codec_;
    const size_t DesiredUncompressedBlockSize_;
    const TTableSchema Schema_;
    const NLogging::TLogger Logger;

    std::vector<TSharedRef> CompressedBlocks_;
    std::unique_ptr<TWireProtocolWriter> WireWriter_;
    bool Closed_ = false;
    bool SchemaWritten_ = false;


    void FlushBlock()
    {
        if (!WireWriter_) {
            return;
        }

        auto uncompressedBlocks = WireWriter_->Finish();

        LOG_DEBUG("Started compressing rowset writer block (BlockIndex: %v, UncompressedSize: %v)",
            CompressedBlocks_.size(),
            GetByteSize(uncompressedBlocks));
        auto compressedBlock = Codec_->Compress(uncompressedBlocks);
        LOG_DEBUG("Finished compressing rowset writer block (BlockIndex: %v, CompressedSize: %v)",
            CompressedBlocks_.size(),
            compressedBlock.Size());

        CompressedBlocks_.push_back(compressedBlock);
        WireWriter_.reset();
    }
};

IWireProtocolRowsetWriterPtr CreateWireProtocolRowsetWriter(
    NCompression::ECodec codecId,
    size_t desiredUncompressedBlockSize,
    const NTableClient::TTableSchema& schema,
    const NLogging::TLogger& logger)
{
    return New<TWireProtocolRowsetWriter>(
        codecId,
        desiredUncompressedBlockSize,
        schema,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

