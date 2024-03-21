#include "arrow.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/assert/assert.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

namespace NYT::NFormats::NArrow {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TParquetAdapter
    : public arrow::io::RandomAccessFile
{
public:
    TParquetAdapter(const TString* metadata, int startMetadataOffset, const std::shared_ptr<IInputStream>& reader)
        : Metadata_(metadata)
        , StartMetadataOffset_(startMetadataOffset)
        , Reader_(reader)
    { }

    arrow::Result<int64_t> GetSize() override
    {
        return Metadata_->size() + StartMetadataOffset_;
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override
    {
        if (FilePosition_ < StartMetadataOffset_) {
            auto bytesRead = Reader_->Load(out, nbytes);
            ReaderPosition_ += bytesRead;
            FilePosition_ += bytesRead;
            return bytesRead;
        }

        auto metadataOffset = FilePosition_ - StartMetadataOffset_;
        memcpy(out, Metadata_->begin() + metadataOffset, nbytes);
        FilePosition_ += nbytes;
        return nbytes;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override
    {
        auto bufferResult = arrow::AllocateResizableBuffer(nbytes);
        ThrowOnError(bufferResult.status());
        auto buffer = std::move(*bufferResult);

        auto bytesReadResult = Read(nbytes, buffer->mutable_data());
        ThrowOnError(bytesReadResult.status());
        auto bytesRead = *bytesReadResult;
        if (bytesRead < nbytes) {
            ThrowOnError(buffer->Resize(bytesRead));
            buffer->ZeroPadding();
        }
        return buffer;
    }

    arrow::Status Seek(int64_t position) override
    {
        if (position >= StartMetadataOffset_) {
            FilePosition_ = position;
        } else {
            if (position < ReaderPosition_) {
                return arrow::Status::Invalid("Attempt at non-monotonous reading from InputStream");
            }
            if (position > ReaderPosition_) {
                auto lenSkip = position - ReaderPosition_;
                while (lenSkip > 0) {
                    auto res = Reader_->Skip(lenSkip);
                    lenSkip -= res;
                    if (res == 0) {
                        return arrow::Status::Invalid("It is impossible to reach position ", position);
                    }
                }
            }
            ReaderPosition_ = position;
            FilePosition_ = position;
        }

        return arrow::Status::OK();
    }

    arrow::Result<int64_t> Tell() const override
    {
        return FilePosition_;
    }

    arrow::Status Close() override
    {
        Closed_ = true;
        return arrow::Status::OK();
    }

    bool closed() const override
    {
        return Closed_;
    }

private:
    const TString* Metadata_;
    const int StartMetadataOffset_;

    std::shared_ptr<IInputStream> Reader_;
    int ReaderPosition_ = 0;
    int FilePosition_ = 0;
    bool Closed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

NTi::TTypePtr GetYtType(const std::shared_ptr<arrow::DataType>& arrowType)
{
    switch (arrowType->id()) {
        case arrow::Type::type::BOOL:
            return NTi::Bool();

        case arrow::Type::type::UINT8:
            return NTi::Uint8();
        case arrow::Type::type::UINT16:
            return NTi::Uint16();
        case arrow::Type::type::UINT32:
            return NTi::Uint32();
        case arrow::Type::type::UINT64:
            return NTi::Uint64();

        case arrow::Type::type::INT8:
            return NTi::Int8();
        case arrow::Type::type::INT16:
            return NTi::Int16();
        case arrow::Type::type::DATE32:
        case arrow::Type::type::TIME32:
        case arrow::Type::type::INT32:
            return NTi::Int32();
        case arrow::Type::type::DATE64:
        case arrow::Type::type::TIMESTAMP:
        case arrow::Type::type::INT64:
        case arrow::Type::type::TIME64:
            return NTi::Int64();

        case arrow::Type::type::HALF_FLOAT:
        case arrow::Type::type::FLOAT:
            return NTi::Float();
        case arrow::Type::type::DOUBLE:
            return NTi::Double();

        case arrow::Type::type::STRING:
        case arrow::Type::type::BINARY:
        case arrow::Type::type::FIXED_SIZE_BINARY:
            return NTi::String();

        case arrow::Type::type::LIST:
            return NTi::List(
                GetYtType(std::reinterpret_pointer_cast<arrow::ListType>(arrowType)->value_type()));

        case arrow::Type::type::MAP:
            return NTi::Dict(
                GetYtType(std::reinterpret_pointer_cast<arrow::MapType>(arrowType)->key_type()),
                GetYtType(std::reinterpret_pointer_cast<arrow::MapType>(arrowType)->item_type()));

        default:
            THROW_ERROR_EXCEPTION("Unsupported arrow type");
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow::Status& status)
{
    if (!status.ok()) {
        THROW_ERROR_EXCEPTION("Arrow error occurred: %Qv", status.message());
    }
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow::io::RandomAccessFile> CreateParquetAdapter(
    const TString* metadata,
    int startMetadataOffset,
    const std::shared_ptr<IInputStream>& reader)
{
    return std::make_shared<TParquetAdapter>(metadata, startMetadataOffset, reader);
}

std::shared_ptr<arrow::Schema> CreateArrowSchemaFromParquetMetadata(const TString* metadata, int startIndex)
{
    auto inputStream = CreateParquetAdapter(metadata, startIndex);
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    auto parquetFileReader = parquet::ParquetFileReader::Open(inputStream);

    std::unique_ptr<parquet::arrow::FileReader> arrowFileReader;

    ThrowOnError(parquet::arrow::FileReader::Make(
            pool,
            std::move(parquetFileReader),
            parquet::ArrowReaderProperties(),
            &arrowFileReader));

    std::shared_ptr<arrow::Schema> arrowSchema;
    ThrowOnError(arrowFileReader->GetSchema(&arrowSchema));
    return arrowSchema;
}

TTableSchema CreateYtTableSchemaFromArrowSchema(const std::shared_ptr<arrow::Schema>& arrowSchema)
{
    TTableSchema resultSchema;
    for (const auto& field : arrowSchema->fields()) {
        auto ytType = NTi::Optional(GetYtType(field->type()));
        resultSchema.AddColumn(TColumnSchema().Name(TString(field->name())).TypeV3(ytType));
    }
    return resultSchema;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats::NArrow
