#include "stdafx.h"
#include "schemaful_chunk_reader.h"
#include "config.h"
#include "private.h"
#include "schemaful_block_reader.h"
#include "name_table.h"
#include "chunk_meta_extensions.h"
#include "schema.h"
#include "schemaful_reader.h"
#include "unversioned_row.h"
#include "schemaless_chunk_reader.h"
#include "schemaful_reader_adapter.h"

#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <core/compression/public.h>

#include <core/misc/async_stream_state.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/chunked_memory_pool.h>

#include <core/concurrency/scheduler.h>

#include <core/logging/log.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <core/rpc/channel.h>

namespace NYT {
namespace NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;

using NChunkClient::NProto::TMiscExt;
using NChunkClient::NProto::TChunkSpec;

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderMemoryPoolTag { };

class TChunkReader
    : public ISchemafulReader
{
public:
    TChunkReader(
        TChunkReaderConfigPtr config,
        NChunkClient::IChunkReaderPtr chunkReader,
        IBlockCachePtr blockCache,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit,
        TTimestamp timestamp);

    virtual bool Read(std::vector<TUnversionedRow>* rows) final override;
    virtual TFuture<void> GetReadyEvent() final override;

    static TFuture<ISchemafulReaderPtr> Create(
        TChunkReaderConfigPtr config,
        NChunkClient::IChunkReaderPtr chunkReader,
        IBlockCachePtr blockCache,
        const TTableSchema& schema,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit,
        TTimestamp timestamp)
    {
        auto reader = New<TChunkReader>(
            std::move(config),
            std::move(chunkReader),
            std::move(blockCache),
            lowerLimit,
            upperLimit,
            timestamp);

        auto nameTable = New<TNameTable>();
        for (int i = 0; i < schema.Columns().size(); ++i) {
            YCHECK(i == nameTable->RegisterName(schema.Columns()[i].Name));
        }

        YCHECK(nameTable);

        reader->NameTable = nameTable;
        reader->Schema = schema;
        reader->IncludeAllColumns = false;

        reader->Logger.AddTag("Reader: %v", reader.Get());
        reader->State.StartOperation();
        TDispatcher::Get()->GetReaderInvoker()->Invoke(BIND(
            &TChunkReader::DoOpen,
            MakeWeak(reader)));

        return reader->State.GetOperationError().Apply(BIND([=] () -> ISchemafulReaderPtr {
            return reader;
        }));
    }

private:
    struct TColumn
    {
        int IndexInBlock;
        int IndexInNameTable;
        int IndexInRow;
    };

    const TChunkReaderConfigPtr Config;
    const NChunkClient::IChunkReaderPtr ChunkReader;
    const IBlockCachePtr BlockCache;

    TTableSchema Schema;
    bool IncludeAllColumns;
    TNameTablePtr NameTable;

    bool IsVersionedChunk;
    TChunkedMemoryPool MemoryPool;
    std::vector<TColumn> FixedColumns;
    std::vector<TColumn> VariableColumns;
    TSequentialReaderPtr SequentialReader;
    std::vector<ui16> ChunkIndexToOutputIndex;

    int CurrentBlockIndex;
    std::unique_ptr<TBlockReader> BlockReader;
    std::vector<EValueType> BlockColumnTypes;

    NProto::TBlockMetaExt BlockMeta;

    TAsyncStreamState State;

    NLogging::TLogger Logger;

    void DoOpen();
    void OnNextBlock(const TError& error);

};

TChunkReader::TChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    TTimestamp timestamp)
    : Config(std::move(config))
    , ChunkReader(std::move(chunkReader))
    , BlockCache(std::move(blockCache))
    , IncludeAllColumns(false)
    , MemoryPool(TChunkReaderMemoryPoolTag())
    , CurrentBlockIndex(0)
    , Logger(TableClientLogger)
{
    YCHECK(timestamp == NullTimestamp);
    YCHECK(IsTrivial(lowerLimit));
    YCHECK(IsTrivial(upperLimit));
}

void TChunkReader::DoOpen()
{
    std::vector<int> extensionTags = {
        TProtoExtensionTag<NProto::TTableSchemaExt>::Value,
        TProtoExtensionTag<NProto::TBlockMetaExt>::Value,
        TProtoExtensionTag<NProto::TNameTableExt>::Value,
        TProtoExtensionTag<TMiscExt>::Value
    };

    LOG_INFO("Requesting chunk meta");
    auto metaOrError = WaitFor(ChunkReader->GetMeta(Null, extensionTags));
    if (!metaOrError.IsOK()) {
        State.Fail(metaOrError);
        return;
    }

    const auto& meta = metaOrError.Value();
    BlockMeta = GetProtoExtension<NProto::TBlockMetaExt>(meta.extensions());
    auto chunkNameTable = NYT::FromProto<TNameTablePtr>(GetProtoExtension<NProto::TNameTableExt>(meta.extensions()));
    auto chunkSchema = NYT::FromProto<TTableSchema>(GetProtoExtension<NProto::TTableSchemaExt>(meta.extensions()));
    auto misc = GetProtoExtension<TMiscExt>(meta.extensions());

    IsVersionedChunk = false;

    // In versioned chunks first column in block is used for timestamp.
    int schemaIndexBase = IsVersionedChunk ? 1 : 0;
    if (IsVersionedChunk) {
        BlockColumnTypes.push_back(EValueType::Int64);
    }

    for (const auto& chunkColumn : chunkSchema.Columns()) {
        BlockColumnTypes.push_back(chunkColumn.Type);
    }

    for (const auto& column : Schema.Columns()) {
        // Validate schema.
        auto* chunkColumn = chunkSchema.FindColumn(column.Name);
        if (!chunkColumn) {
            State.Fail(TError(
                "Chunk schema doesn't contain column %Qv",
                column.Name));
            return;
        }

        if (chunkColumn->Type != column.Type) {
            State.Fail(TError(
                "Chunk schema column %Qv has incompatible type: expected %Qlv, actual %Qlv",
                column.Name,
                column.Type,
                chunkColumn->Type));
            return;
        }

        // Fill FixedColumns.
        TColumn fixedColumn;
        fixedColumn.IndexInBlock = schemaIndexBase + chunkSchema.GetColumnIndex(*chunkColumn);
        fixedColumn.IndexInRow = FixedColumns.size();
        fixedColumn.IndexInNameTable = NameTable->GetIdOrRegisterName(column.Name);
        FixedColumns.push_back(fixedColumn);
    }

    if (IncludeAllColumns) {
        for (int i = 0; i < chunkSchema.Columns().size(); ++i) {
            const auto& chunkColumn = chunkSchema.Columns()[i];
            if (!Schema.FindColumn(chunkColumn.Name)) {
                TColumn variableColumn;
                variableColumn.IndexInBlock = schemaIndexBase + i;
                variableColumn.IndexInRow = FixedColumns.size() + VariableColumns.size();
                variableColumn.IndexInNameTable = NameTable->GetIdOrRegisterName(chunkColumn.Name);
                VariableColumns.push_back(variableColumn);
            }
        }

        ChunkIndexToOutputIndex.resize(chunkNameTable->GetSize());
        for (int i = 0; i < chunkNameTable->GetSize(); ++i) {
            ChunkIndexToOutputIndex[i] = NameTable->GetIdOrRegisterName(chunkNameTable->GetName(i));
        }
    }

    std::vector<TSequentialReader::TBlockInfo> blockSequence;
    {
        // ToDo(psushin): Choose proper blocks and rows using index.
        for (int i = 0; i < BlockMeta.blocks_size(); ++i) {
            const auto& blockMeta = BlockMeta.blocks(i);
            blockSequence.push_back(TSequentialReader::TBlockInfo(i, blockMeta.uncompressed_size()));
        }
    }

    SequentialReader = New<TSequentialReader>(
        Config,
        std::move(blockSequence),
        ChunkReader,
        BlockCache,
        NCompression::ECodec(misc.compression_codec()));

    if (SequentialReader->HasMoreBlocks()) {
        auto error = WaitFor(SequentialReader->FetchNextBlock());
        if (error.IsOK()) {
            BlockReader.reset(new TBlockReader(
                BlockMeta.blocks(CurrentBlockIndex),
                SequentialReader->GetCurrentBlock(),
                BlockColumnTypes));
        }
        State.FinishOperation(error);
    } else {
        State.Close();
    }
}

bool TChunkReader::Read(std::vector<TUnversionedRow> *rows)
{
    rows->clear();

    if (!State.IsActive()) {
        const auto& error = State.GetCurrentError();
        return !error.IsOK();
    }

    if (BlockReader->EndOfBlock()) {
        YCHECK(!State.HasRunningOperation());
        ++CurrentBlockIndex;
        BlockReader.reset(new TBlockReader(
            BlockMeta.blocks(CurrentBlockIndex),
            SequentialReader->GetCurrentBlock(),
            BlockColumnTypes));
    }

    MemoryPool.Clear();
    while (rows->size() < rows->capacity()) {
        if (IsVersionedChunk && !BlockReader->GetEndOfKeyFlag()) {
            continue;
        }

        if (IncludeAllColumns) {
            auto variableIt = BlockReader->GetVariableIterator();

            rows->push_back(TUnversionedRow::Allocate(
                &MemoryPool,
                FixedColumns.size() + VariableColumns.size() + variableIt.GetRemainingCount()));

            auto& row = rows->back();
            for (const auto& column : VariableColumns) {
                auto value = BlockReader->Read(column.IndexInBlock);
                value.Id = column.IndexInNameTable;
                row[column.IndexInRow] = value;
            }

            for (int index = FixedColumns.size() + VariableColumns.size(); index < row.GetCount(); ++index) {
                TUnversionedValue value;
                YCHECK(variableIt.ParseNext(&value));
                value.Id = ChunkIndexToOutputIndex[value.Id];
                row[index] = value;
            }
        } else {
            rows->push_back(TUnversionedRow::Allocate(&MemoryPool, FixedColumns.size()));
        }

        auto& row = rows->back();
        for (const auto& column : FixedColumns) {
            auto value = BlockReader->Read(column.IndexInBlock);
            value.Id = column.IndexInNameTable;
            row[column.IndexInRow] = value;
        }

        BlockReader->NextRow();
        if (BlockReader->EndOfBlock()) {
            if (SequentialReader->HasMoreBlocks()) {
                State.StartOperation();
                SequentialReader->FetchNextBlock().Subscribe(BIND(
                    &TChunkReader::OnNextBlock,
                    MakeWeak(this)));
            } else {
                State.Close();
            }
            break;
        }
    }

    return true;
}

TFuture<void> TChunkReader::GetReadyEvent()
{
    return State.GetOperationError();
}

void TChunkReader::OnNextBlock(const TError& error)
{
    State.FinishOperation(error);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<ISchemafulReaderPtr> CreateSchemafulChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    IBlockCachePtr blockCache,
    const TTableSchema& schema,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    TTimestamp timestamp)
{
    auto type = EChunkType(chunkMeta.type());
    YCHECK(type == EChunkType::Table);

    auto formatVersion = ETableChunkFormat(chunkMeta.version());
    switch (formatVersion) {
        case ETableChunkFormat::Old:
        case ETableChunkFormat::SchemalessHorizontal: {
            auto createSchemalessReader = [=] (TNameTablePtr nameTable, TColumnFilter columnFilter) {
                return CreateSchemalessChunkReader(
                    std::move(config),
                    std::move(chunkReader),
                    std::move(nameTable),
                    std::move(blockCache),
                    TKeyColumns(),
                    chunkMeta,
                    lowerLimit,
                    upperLimit,
                    columnFilter);
            };

            return CreateSchemafulReaderAdapter(createSchemalessReader, schema);
        }

        case ETableChunkFormat::Schemaful: {
            return TChunkReader::Create(std::move(config),
                std::move(chunkReader),
                std::move(blockCache),
                schema,
                lowerLimit,
                upperLimit,
                timestamp);
        }
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
