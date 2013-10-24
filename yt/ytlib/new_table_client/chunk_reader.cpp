#include "stdafx.h"
#include "chunk_reader.h"
#include "reader.h"
#include "config.h"
#include "private.h"
#include "block_reader.h"
#include "name_table.h"
#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <core/compression/public.h>

#include <core/misc/async_stream_state.h>
#include <core/concurrency/fiber.h>

// TableChunkReaderAdapter stuff
#include <ytlib/chunk_client/multi_chunk_sequential_reader.h>
#include <ytlib/chunk_client/config.h>

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/config.h>
#include <ytlib/table_client/table_chunk_reader.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <core/rpc/channel.h>
#include <core/yson/tokenizer.h>


namespace NYT {
namespace NVersionedTableClient {

using namespace NProto;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

// TableChunkReaderAdapter stuff
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TChunkReader
    : public IReader
{
public:
    TChunkReader(
        TChunkReaderConfigPtr config,
        NChunkClient::IAsyncReaderPtr asyncReader,
        const TReadLimit& startLimit,
        const TReadLimit& endLimit,
        TTimestamp timestamp);

    TAsyncError Open(
        TNameTablePtr nameTable, 
        const TTableSchemaExt& schema,
        bool includeAllColumns,
        ERowsetType type) override;

    virtual bool Read(std::vector<TRow>* rows) override;
    virtual TAsyncError GetReadyEvent() override;

private:
    struct TColumn
    {
        int IndexInBlock;
        int IndexInNameTable;
        int IndexInRow;
    };

    TChunkReaderConfigPtr Config;
    NChunkClient::IAsyncReaderPtr UnderlyingReader;

    TTableSchemaExt Schema;
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
    std::vector<EColumnType> BlockColumnTypes;

    TBlockMetaExt BlockMeta;

    TAsyncStreamState State;

    NLog::TTaggedLogger Logger;

    void DoOpen();
    void OnNextBlock(TError error);

};

TChunkReader::TChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IAsyncReaderPtr asyncReader,
    const TReadLimit& startLimit,
    const TReadLimit& endLimit,
    TTimestamp timestamp)
    : Config(config)
    , UnderlyingReader(asyncReader)
    , IncludeAllColumns(false)
    , CurrentBlockIndex(0)
    , Logger(TableReaderLogger)
{
    YCHECK(timestamp == NullTimestamp);
    YCHECK(IsTrivial(startLimit));
    YCHECK(IsTrivial(endLimit));
}

TAsyncError TChunkReader::Open(
    TNameTablePtr nameTable,
    const TTableSchemaExt& schema,
    bool includeAllColumns,
    ERowsetType type)
{
    YCHECK(nameTable);
    YCHECK(type == ERowsetType::Simple);

    NameTable = nameTable;
    Schema = schema;
    IncludeAllColumns = includeAllColumns;

    Logger.AddTag(Sprintf("Reader: %p", this));
    State.StartOperation();
    TDispatcher::Get()->GetReaderInvoker()->Invoke(BIND(
        &TChunkReader::DoOpen,
        MakeWeak(this)));

    return State.GetOperationError();
}

void TChunkReader::DoOpen()
{
    std::vector<int> tags;
    tags.push_back(TProtoExtensionTag<TTableSchemaExt>::Value);
    tags.push_back(TProtoExtensionTag<TBlockMetaExt>::Value);
    tags.push_back(TProtoExtensionTag<TNameTableExt>::Value);
    tags.push_back(TProtoExtensionTag<TMiscExt>::Value);

    LOG_INFO("Requesting chunk meta");
    auto metaOrError = WaitFor(UnderlyingReader->AsyncGetChunkMeta(Null, &tags));
    if (!metaOrError.IsOK()) {
        State.Finish(metaOrError);
        return;
    }

    const auto& meta = metaOrError.GetValue();
    BlockMeta = GetProtoExtension<TBlockMetaExt>(meta.extensions());
    auto chunkNameTable = FromProto(GetProtoExtension<TNameTableExt>(meta.extensions()));
    auto chunkSchema = GetProtoExtension<TTableSchemaExt>(meta.extensions());
    auto misc = GetProtoExtension<TMiscExt>(meta.extensions());

    IsVersionedChunk = misc.versioned();

    // In versioned chunks first column in block is used for timestamp.
    int schemaIndexBase = IsVersionedChunk ? 1 : 0;
    if (IsVersionedChunk) {
        BlockColumnTypes.push_back(EColumnType::Integer);
    }

    for (const auto& chunkColumn: chunkSchema.columns()) {
        BlockColumnTypes.push_back(EColumnType(chunkColumn.type()));
    }

    for (const auto& column: Schema.columns()) {
        // Validate schema.
        auto chunkType = FindColumnType(column.name(), chunkSchema);
        auto schemaType = EColumnType(column.type());
        if (!chunkType) {
            State.Finish(TError(
                "Chunk schema doesn't contain column %s",
                ~column.name().Quote()));
            return;
        } else if (*chunkType != schemaType) {
            State.Finish(TError(
                "Chunk schema column %s has incompatible type (ChunkColumnType: %s, ReadingColumnType: %s)",
                ~column.name().Quote(),
                ~FormatEnum(*chunkType),
                ~FormatEnum(schemaType)));
            return;
        }

        // Fill FixedColumns.
        TColumn fixedColumn;
        fixedColumn.IndexInBlock = schemaIndexBase + GetColumnIndex(column.name(), chunkSchema);
        fixedColumn.IndexInRow = FixedColumns.size();
        fixedColumn.IndexInNameTable = NameTable->GetOrRegister(column.name());
        FixedColumns.push_back(fixedColumn);
    }

    if (IncludeAllColumns) {
        for (int i = 0; i < chunkSchema.columns_size(); ++i) {
            const auto& chunkColumn = chunkSchema.columns(i);
            if (!FindColumnIndex(chunkColumn.name(), Schema)) {
                TColumn variableColumn;
                variableColumn.IndexInBlock = schemaIndexBase + i;
                variableColumn.IndexInRow = FixedColumns.size() + VariableColumns.size();
                variableColumn.IndexInNameTable = NameTable->GetOrRegister(chunkColumn.name());
                VariableColumns.push_back(variableColumn);
            }
        }

        ChunkIndexToOutputIndex.reserve(chunkNameTable->GetNameCount());
        for (int i = 0; i < chunkNameTable->GetNameCount(); ++i) {
            ChunkIndexToOutputIndex[i] = NameTable->GetOrRegister(chunkNameTable->GetName(i));
        }
    }

    std::vector<TSequentialReader::TBlockInfo> blockSequence;
    {
        // ToDo: Choose proper blocks and rows using index.
        for (int i = 0; i < BlockMeta.items_size(); ++i) {
            const auto& blockMeta = BlockMeta.items(i);
            blockSequence.push_back(TSequentialReader::TBlockInfo(i, blockMeta.block_size()));
        }
    }

    SequentialReader = New<TSequentialReader>(
        Config,
        std::move(blockSequence),
        UnderlyingReader,
        NCompression::ECodec(misc.compression_codec()));

    if (SequentialReader->HasNext()) {
        auto error = WaitFor(SequentialReader->AsyncNextBlock());
        if (error.IsOK()) {
            BlockReader.reset(new TBlockReader(
                BlockMeta.items(CurrentBlockIndex),
                SequentialReader->GetBlock(),
                BlockColumnTypes));
        }
        State.FinishOperation(error);
    } else {
        State.Close();
    }
}

bool TChunkReader::Read(std::vector<TRow> *rows)
{
    YCHECK(rows->empty());

    if (!State.IsActive()) {
        const auto& error = State.GetCurrentError();
        return !error.IsOK();
    }

    if (BlockReader->EndOfBlock()) {
        YCHECK(!State.HasRunningOperation());
        ++CurrentBlockIndex;
        BlockReader.reset(new TBlockReader(
            BlockMeta.items(CurrentBlockIndex),
            SequentialReader->GetBlock(),
            BlockColumnTypes));
    }

    MemoryPool.Clear();
    while (rows->size() < rows->capacity()) {
        if (IsVersionedChunk && !BlockReader->GetEndOfKeyFlag()) {
            continue;
        }

        if (IncludeAllColumns) {
            auto variableIt = BlockReader->GetVariableIterator();

            rows->push_back(TRow(&MemoryPool, FixedColumns.size() +
                VariableColumns.size() + variableIt.GetLeftValueCount()));

            auto& row = rows->back();
            for (const auto& column: VariableColumns) {
                row[column.IndexInRow] = BlockReader->Read(column.IndexInBlock);
                row[column.IndexInRow].Index = column.IndexInNameTable;
            }

            int index = FixedColumns.size() + VariableColumns.size();
            while (variableIt.Next(&row[index])) {
                row[index].Index = ChunkIndexToOutputIndex[row[index].Index];
            }
        } else {
            rows->push_back(TRow(&MemoryPool, FixedColumns.size()));
        }

        auto& row = rows->back();
        for (const auto& column: FixedColumns) {
            row[column.IndexInRow] = BlockReader->Read(column.IndexInBlock);
            row[column.IndexInRow].Index = column.IndexInNameTable;
        }

        BlockReader->NextRow();
        if (BlockReader->EndOfBlock()) {
            if (SequentialReader->HasNext()) {
                State.StartOperation();
                SequentialReader->AsyncNextBlock().Subscribe(BIND(
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

TAsyncError TChunkReader::GetReadyEvent()
{
    return State.GetOperationError();
}

void TChunkReader::OnNextBlock(TError error)
{
    State.FinishOperation(error);
}

////////////////////////////////////////////////////////////////////////////////

IReaderPtr CreateChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IAsyncReaderPtr asyncReader,
    const TReadLimit& startLimit,
    const TReadLimit& endLimit,
    TTimestamp timestamp)
{
    return New<TChunkReader>(config, asyncReader, startLimit, endLimit, timestamp);
}

////////////////////////////////////////////////////////////////////////////////

// Adapter for old chunks.
class TTableChunkReaderAdapter
    : public IReader
{
public:
    TTableChunkReaderAdapter(TTableChunkSequenceReaderPtr underlyingReader);

    TAsyncError Open(
        TNameTablePtr nameTable,
        const TTableSchemaExt& schema,
        bool includeAllColumns,
        ERowsetType type) override;

    virtual bool Read(std::vector<TRow>* rows) override;
    virtual TAsyncError GetReadyEvent() override;

private:
    TTableChunkSequenceReaderPtr UnderlyingReader;

    bool IncludeAllColumns;
    TTableSchemaExt Schema;
    TNameTablePtr NameTable;
    std::vector<int> SchemaNameIndexes;

    TChunkedMemoryPool MemoryPool;

    void ThrowIncompatibleType(const Stroka& columnName);
};

TTableChunkReaderAdapter::TTableChunkReaderAdapter(
    TTableChunkSequenceReaderPtr underlyingReader)
    : UnderlyingReader(underlyingReader)
    , IncludeAllColumns(false)
{ }

TAsyncError TTableChunkReaderAdapter::Open(
    TNameTablePtr nameTable,
    const TTableSchemaExt &schema,
    bool includeAllColumns,
    ERowsetType type)
{
    YCHECK(type == ERowsetType::Simple);
    IncludeAllColumns = includeAllColumns;
    Schema = schema;
    NameTable = nameTable;

    SchemaNameIndexes.reserve(Schema.columns_size());
    for (const auto& column: Schema.columns()) {
        SchemaNameIndexes.push_back(NameTable->GetOrRegister(column.name()));
    }

    return UnderlyingReader->AsyncOpen();
}

bool TTableChunkReaderAdapter::Read(std::vector<TRow> *rows)
{
    YCHECK(rows->capacity() > 0);

    std::vector<int> schemaIndexes;
    std::vector<int> variableIndexes;

    while (rows->size() < rows->capacity()) {
        auto* facade = UnderlyingReader->GetFacade();
        if (!facade) {
            return false;
        }

        schemaIndexes.resize(Schema.columns_size(), -1);
        auto& chunkRow = facade->GetRow();
        for (int i = 0; i < chunkRow.size(); ++i) {
            auto indexInSchema = FindColumnIndex(chunkRow[i].first, Schema);
            if (indexInSchema) {
                schemaIndexes[*indexInSchema] =  i;
            } else if (IncludeAllColumns) {
                variableIndexes.push_back(i);
            }
        }

        rows->push_back(TRow(&MemoryPool, Schema.columns_size() + variableIndexes.size()));
        auto& outputRow = rows->back();

        for (int i = 0; i < schemaIndexes.size(); ++i) {
            if (schemaIndexes[i] < 0) {
                outputRow[i] = TRowValue();
            } else {
                auto& value = outputRow[i];
                value.Index = SchemaNameIndexes[i];
                value.Type = Schema.columns(i).type();

                const auto& pair = chunkRow[schemaIndexes[i]];

                if (value.Type == EColumnType::Any) {
                    value.Data.Any = pair.second.begin();
                    value.Length = pair.second.size();
                    continue;
                }

                NYson::TStatelessLexer lexer;
                NYson::TToken token;
                lexer.GetToken(pair.second, &token);
                YCHECK(!token.IsEmpty());

                switch (value.Type) {
                case EColumnType::Integer:
                    if (token.GetType() != ETokenType::Integer) {
                        ThrowIncompatibleType(Schema.columns(i).name());
                    }
                    value.Data.Integer = token.GetIntegerValue();
                    break;

                case EColumnType::Double:
                    if (token.GetType() != ETokenType::Double) {
                        ThrowIncompatibleType(Schema.columns(i).name());
                    }
                    value.Data.Double = token.GetDoubleValue();
                    break;

                case EColumnType::String:
                    if (token.GetType() != ETokenType::String) {
                        ThrowIncompatibleType(Schema.columns(i).name());
                    }
                    value.Length = token.GetStringValue().size();
                    value.Data.String = token.GetStringValue().begin();
                    break;

                default:
                    YUNREACHABLE();
                }
            }
        }

        for (int i = 0; i < variableIndexes.size(); ++i) {
            auto& value = outputRow[schemaIndexes.size() + i];
            const auto& pair = chunkRow[variableIndexes[i]];

            value.Index = NameTable->GetOrRegister(ToString(pair.first));
            value.Type = EColumnType::Any;
            value.Data.Any = pair.second.begin();
            value.Length = pair.second.size();
        }

        if (!UnderlyingReader->FetchNext()) {
            return true;
        }

        schemaIndexes.clear();
        variableIndexes.clear();
    }

    return true;
}

TAsyncError TTableChunkReaderAdapter::GetReadyEvent()
{
    return UnderlyingReader->GetReadyEvent();
}

void TTableChunkReaderAdapter::ThrowIncompatibleType(const Stroka& columnName)
{
    THROW_ERROR_EXCEPTION(
        "Chunk data in column %s is incompatible with schema",
        ~columnName.Quote());
}

////////////////////////////////////////////////////////////////////////////////

IReaderPtr CreateChunkReader(
    TChunkReaderConfigPtr config,
    const TChunkSpec& chunkSpec,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IBlockCachePtr blockCache,
    TTimestamp timestamp)
{
    std::vector<TChunkSpec> chunkSpecs;
    chunkSpecs.push_back(chunkSpec);

    auto provider = New<TTableChunkReaderProvider>(
        chunkSpecs,
        config,
        New<TChunkReaderOptions>());

    auto multiChunkReaderConfig = New<TMultiChunkReaderConfig>();
    auto reader = New<TTableChunkSequenceReader>(
        multiChunkReaderConfig,
        masterChannel,
        blockCache,
        nodeDirectory,
        std::move(chunkSpecs),
        provider);

    return New<TTableChunkReaderAdapter>(reader);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
