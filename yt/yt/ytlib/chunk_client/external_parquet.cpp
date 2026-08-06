#include "external_parquet.h"

#include "chunk_meta_extensions.h"

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/client/arrow/schema.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/library/erasure/public.h>

#include <contrib/libs/apache/arrow_next/cpp/src/parquet/arrow/reader.h>

#include <algorithm>
#include <cstring>
#include <limits>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TS3ArrowRandomAccessFile::TS3ArrowRandomAccessFile(NS3::TObjectDescriptor object, NS3::IClientPtr client)
    : Object_(std::move(object))
    , Client_(std::move(client))
    , FileSize_(WaitFor(Client_->HeadObject({.Bucket = Object_.Bucket(), .Key = Object_.Key()}))
        .ValueOrThrow()
        .Size)
{ }

arrow20::Result<int64_t> TS3ArrowRandomAccessFile::GetSize()
{
    return FileSize_;
}

arrow20::Result<int64_t> TS3ArrowRandomAccessFile::ReadAt(int64_t position, int64_t nbytes, void* out)
{
    if (position < 0 || position > FileSize_) {
        return arrow20::Status::Invalid(Format(
            "Read position %v is outside file bounds [0, %v)", position, FileSize_));
    }

    nbytes = std::min(nbytes, FileSize_ - position);
    if (nbytes <= 0) {
        return 0;
    }

    auto response = WaitFor(Client_->GetObject({
        .Bucket = Object_.Bucket(),
        .Key = Object_.Key(),
        .Range = Format("bytes=%v-%v", position, position + nbytes - 1),
    })).ValueOrThrow();
    const auto bytesRead = std::min<i64>(nbytes, response.Data.Size());
    std::memcpy(out, response.Data.Begin(), bytesRead);
    return bytesRead;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TParquetChunkMetaGenerator
    : public ITableChunkMetaGenerator
{
public:
    explicit TParquetChunkMetaGenerator(std::shared_ptr<arrow20::io::RandomAccessFile> chunkFile)
        : ChunkFile_(std::move(chunkFile))
    { }

    void Generate() override
    {
        PARQUET_ASSIGN_OR_THROW(ArrowReader_, parquet20::arrow20::OpenFile(ChunkFile_, arrow20::default_memory_pool()));
        ParquetMeta_ = ArrowReader_->parquet_reader()->metadata();
        PARQUET_THROW_NOT_OK(ArrowReader_->GetSchema(&ArrowSchema_));
        Schema_ = NArrow::CreateYTTableSchemaFromArrowSchema(ArrowSchema_);
        PARQUET_ASSIGN_OR_THROW(FileSize_, ChunkFile_->GetSize());

        PrepareBlocks();
        ChunkMeta_ = BuildChunkMeta();
    }

    i64 GetRowCount() const override
    {
        return ParquetMeta_->num_rows();
    }

    i64 GetUncompressedDataSize() const override
    {
        return FileSize_;
    }

    i64 GetCompressedDataSize() const override
    {
        return FileSize_;
    }

    TTableSchemaPtr GetChunkSchema() const override
    {
        return Schema_;
    }

    TRefCountedChunkMetaPtr GetChunkMeta() const override
    {
        return ChunkMeta_;
    }

private:
    struct TBlock
    {
        i64 Offset;
        i64 Size;
        i64 RowCount;
    };

    const std::shared_ptr<arrow20::io::RandomAccessFile> ChunkFile_;
    std::unique_ptr<parquet20::arrow20::FileReader> ArrowReader_;
    std::shared_ptr<parquet20::FileMetaData> ParquetMeta_;
    std::shared_ptr<arrow20::Schema> ArrowSchema_;
    TTableSchemaPtr Schema_;
    std::vector<TBlock> Blocks_;
    i64 FileSize_ = 0;
    i64 MaxBlockSize_ = 0;
    TRefCountedChunkMetaPtr ChunkMeta_;

    static i64 GetMinimumDataOffset(const parquet20::ColumnChunkMetaData& column)
    {
        i64 result = std::numeric_limits<i64>::max();
        for (const auto offset : {
                column.file_offset(),
                column.data_page_offset(),
                column.dictionary_page_offset(),
                column.index_page_offset(),
            })
        {
            if (offset > 0) {
                result = std::min(result, offset);
            }
        }
        return result;
    }

    void PrepareBlocks()
    {
        std::vector<i64> offsets;
        offsets.reserve(ParquetMeta_->num_row_groups() + 1);

        for (int rowGroupIndex = 0; rowGroupIndex < ParquetMeta_->num_row_groups(); ++rowGroupIndex) {
            const auto rowGroup = ParquetMeta_->RowGroup(rowGroupIndex);
            i64 offset = std::numeric_limits<i64>::max();
            for (int columnIndex = 0; columnIndex < rowGroup->num_columns(); ++columnIndex) {
                offset = std::min(offset, GetMinimumDataOffset(*rowGroup->ColumnChunk(columnIndex)));
            }
            THROW_ERROR_EXCEPTION_IF(
                offset == std::numeric_limits<i64>::max(),
                "Cannot determine data offset of Parquet row group %v",
                rowGroupIndex);
            offsets.push_back(offset);
        }

        constexpr i64 ParquetFooterTrailerSize = 8; // metadata length plus PAR1.
        const auto footerOffset = FileSize_ - ParquetMeta_->size() - ParquetFooterTrailerSize;
        THROW_ERROR_EXCEPTION_IF(footerOffset < 4, "Invalid Parquet footer offset %v", footerOffset);
        offsets.push_back(footerOffset);

        Blocks_.reserve(ParquetMeta_->num_row_groups());
        for (int rowGroupIndex = 0; rowGroupIndex < ParquetMeta_->num_row_groups(); ++rowGroupIndex) {
            const auto startOffset = offsets[rowGroupIndex];
            const auto endOffset = offsets[rowGroupIndex + 1];
            THROW_ERROR_EXCEPTION_IF(
                startOffset < 0 || endOffset > FileSize_ || startOffset >= endOffset,
                "Invalid Parquet row group bounds [%v, %v) for file of size %v",
                startOffset,
                endOffset,
                FileSize_);

            const auto size = endOffset - startOffset;
            Blocks_.push_back({startOffset, size, ParquetMeta_->RowGroup(rowGroupIndex)->num_rows()});
            MaxBlockSize_ = std::max(MaxBlockSize_, size);
        }
    }

    TRefCountedChunkMetaPtr BuildChunkMeta() const
    {
        auto meta = New<TRefCountedChunkMeta>();
        meta->set_type(ToProto(EChunkType::Table));
        meta->set_format(ToProto(EChunkFormat::TableUnversionedArrowParquet));

        NChunkClient::NProto::TBlocksExt blocksExt;
        NTableClient::NProto::TDataBlockMetaExt dataBlockMetaExt;
        i64 accumulatedRowCount = 0;
        for (int index = 0; index < std::ssize(Blocks_); ++index) {
            const auto& block = Blocks_[index];
            auto* blockInfo = blocksExt.add_blocks();
            blockInfo->set_offset(block.Offset);
            blockInfo->set_size(block.Size);
            blockInfo->set_checksum(NullChecksum);

            accumulatedRowCount += block.RowCount;
            auto* dataBlockMeta = dataBlockMetaExt.add_data_blocks();
            dataBlockMeta->set_row_count(block.RowCount);
            dataBlockMeta->set_chunk_row_count(accumulatedRowCount);
            dataBlockMeta->set_uncompressed_size(block.Size);
            dataBlockMeta->set_block_index(index);
        }
        SetProtoExtension(meta->mutable_extensions(), blocksExt);
        SetProtoExtension(meta->mutable_extensions(), dataBlockMetaExt);

        NTableClient::NProto::TNameTableExt nameTableExt;
        ToProto(&nameTableExt, TNameTable::FromSchema(*Schema_));
        SetProtoExtension(meta->mutable_extensions(), nameTableExt);

        NTableClient::NProto::TTableSchemaExt tableSchemaExt;
        ToProto(&tableSchemaExt, *Schema_);
        SetProtoExtension(meta->mutable_extensions(), tableSchemaExt);

        NTableClient::NProto::TParquetFormatMetaExt parquetFormatMetaExt;
        parquetFormatMetaExt.set_footer(ParquetMeta_->SerializeToString());
        parquetFormatMetaExt.set_file_size(FileSize_);
        SetProtoExtension(meta->mutable_extensions(), parquetFormatMetaExt);

        NChunkClient::NProto::TMiscExt miscExt;
        miscExt.set_uncompressed_data_size(FileSize_);
        miscExt.set_compressed_data_size(FileSize_);
        miscExt.set_data_weight(FileSize_);
        miscExt.set_meta_size(meta->ByteSizeLong());
        miscExt.set_row_count(GetRowCount());
        miscExt.set_compression_codec(ToProto(NCompression::ECodec::None));
        miscExt.set_sorted(false);
        miscExt.set_max_data_block_size(MaxBlockSize_);
        miscExt.set_sealed(false);
        miscExt.set_erasure_codec(ToProto(NErasure::ECodec::None));
        miscExt.set_system_block_count(0);
        miscExt.set_striped_erasure(false);
        SetProtoExtension(meta->mutable_extensions(), miscExt);

        return meta;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ITableChunkMetaGeneratorPtr CreateArrowTableChunkMetaGenerator(
    EChunkFormat chunkFormat,
    std::shared_ptr<arrow20::io::RandomAccessFile> chunkFile,
    TArrowTableChunkMetaGeneratorOptions /*options*/)
{
    THROW_ERROR_EXCEPTION_IF(
        chunkFormat != EChunkFormat::TableUnversionedArrowParquet,
        "Unsupported chunk format %Qlv for external Arrow table metadata generation",
        chunkFormat);
    return New<TParquetChunkMetaGenerator>(std::move(chunkFile));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
