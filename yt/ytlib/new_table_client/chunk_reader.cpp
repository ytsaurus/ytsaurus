#include "stdafx.h"
#include "chunk_reader.h"
#include "reader.h"
#include "config.h"
#include "private.h"
#include "block_reader.h"
#include "name_table.h"
#include "chunk_meta_extensions.h"
#include "schema.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <core/compression/public.h>

#include <core/misc/async_stream_state.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/chunked_memory_pool.h>

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

using namespace NConcurrency;
using namespace NChunkClient;
using NChunkClient::NProto::TMiscExt;
using NChunkClient::NProto::TChunkSpec;

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
        const TTableSchema& schema,
        bool includeAllColumns,
        ERowsetType type) override;

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;
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
    const TTableSchema& schema,
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
    tags.push_back(TProtoExtensionTag<NProto::TTableSchemaExt>::Value);
    tags.push_back(TProtoExtensionTag<NProto::TBlockMetaExt>::Value);
    tags.push_back(TProtoExtensionTag<NProto::TNameTableExt>::Value);
    tags.push_back(TProtoExtensionTag<TMiscExt>::Value);

    LOG_INFO("Requesting chunk meta");
    auto metaOrError = WaitFor(UnderlyingReader->AsyncGetChunkMeta(Null, &tags));
    if (!metaOrError.IsOK()) {
        State.Finish(metaOrError);
        return;
    }

    const auto& meta = metaOrError.GetValue();
    BlockMeta = GetProtoExtension<NProto::TBlockMetaExt>(meta.extensions());
    auto chunkNameTable = NYT::FromProto<TNameTablePtr>(GetProtoExtension<NProto::TNameTableExt>(meta.extensions()));
    auto chunkSchema = NYT::FromProto<TTableSchema>(GetProtoExtension<NProto::TTableSchemaExt>(meta.extensions()));
    auto misc = GetProtoExtension<TMiscExt>(meta.extensions());

    IsVersionedChunk = misc.versioned();

    // In versioned chunks first column in block is used for timestamp.
    int schemaIndexBase = IsVersionedChunk ? 1 : 0;
    if (IsVersionedChunk) {
        BlockColumnTypes.push_back(EValueType::Integer);
    }

    for (const auto& chunkColumn : chunkSchema.Columns()) {
        BlockColumnTypes.push_back(chunkColumn.Type);
    }

    for (const auto& column : Schema.Columns()) {
        // Validate schema.
        auto* chunkColumn = chunkSchema.FindColumn(column.Name);
        if (!chunkColumn) {
            State.Finish(TError(
                "Chunk schema doesn't contain column %s",
                ~column.Name.Quote()));
            return;
        }
        
        if (chunkColumn->Type != column.Type) {
            State.Finish(TError(
                "Chunk schema column %s has incompatible type: expected %s, actual %s",
                ~column.Name.Quote(),
                ~FormatEnum(column.Type),
                ~FormatEnum(chunkColumn->Type)));
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

bool TChunkReader::Read(std::vector<TUnversionedRow> *rows)
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
                YASSERT(variableIt.ParseNext(&value));
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
        const TTableSchema& schema,
        bool includeAllColumns,
        ERowsetType type) override;

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;
    virtual TAsyncError GetReadyEvent() override;

private:
    TTableChunkSequenceReaderPtr UnderlyingReader;

    bool IncludeAllColumns;
    TTableSchema Schema;
    TNameTablePtr NameTable;
    std::vector<int> SchemaNameIndexes;

    TChunkedMemoryPool MemoryPool;

    void ThrowIncompatibleType(const TColumnSchema& schema);

};

TTableChunkReaderAdapter::TTableChunkReaderAdapter(
    TTableChunkSequenceReaderPtr underlyingReader)
    : UnderlyingReader(underlyingReader)
    , IncludeAllColumns(false)
{ }

TAsyncError TTableChunkReaderAdapter::Open(
    TNameTablePtr nameTable,
    const TTableSchema& schema,
    bool includeAllColumns,
    ERowsetType type)
{
    YCHECK(type == ERowsetType::Simple);
    IncludeAllColumns = includeAllColumns;
    Schema = schema;
    NameTable = nameTable;

    SchemaNameIndexes.reserve(Schema.Columns().size());
    for (const auto& column : Schema.Columns()) {
        SchemaNameIndexes.push_back(NameTable->GetIdOrRegisterName(column.Name));
    }

    return UnderlyingReader->AsyncOpen();
}

bool TTableChunkReaderAdapter::Read(std::vector<TUnversionedRow> *rows)
{
    YCHECK(rows->capacity() > 0);

    std::vector<int> schemaIndexes;
    std::vector<int> variableIndexes;

    while (rows->size() < rows->capacity()) {
        auto* facade = UnderlyingReader->GetFacade();
        if (!facade) {
            return false;
        }

        schemaIndexes.resize(Schema.Columns().size(), -1);
        auto& chunkRow = facade->GetRow();
        for (int i = 0; i < chunkRow.size(); ++i) {
            auto* schemaColumn = Schema.FindColumn(chunkRow[i].first);
            if (schemaColumn) {
                int schemaIndex = Schema.GetColumnIndex(*schemaColumn);
                schemaIndexes[schemaIndex] =  i;
            } else if (IncludeAllColumns) {
                variableIndexes.push_back(i);
            }
        }

        rows->push_back(TUnversionedRow::Allocate(&MemoryPool, Schema.Columns().size() + variableIndexes.size()));
        auto& outputRow = rows->back();

        for (int i = 0; i < schemaIndexes.size(); ++i) {
            if (schemaIndexes[i] < 0) {
                outputRow[i].Type = EValueType::Null;
            } else {
                const auto& schemaColumn = Schema.Columns()[i];
                auto& value = outputRow[i];
                value.Id = SchemaNameIndexes[i];
                value.Type = schemaColumn.Type;

                const auto& pair = chunkRow[schemaIndexes[i]];

                if (value.Type == EValueType::Any) {
                    value.Data.String = pair.second.begin();
                    value.Length = pair.second.size();
                    continue;
                }

                NYson::TStatelessLexer lexer;
                NYson::TToken token;
                lexer.GetToken(pair.second, &token);
                YCHECK(!token.IsEmpty());

                switch (value.Type) {
                    case EValueType::Integer:
                        if (token.GetType() != ETokenType::Integer) {
                            ThrowIncompatibleType(schemaColumn);
                        }
                        value.Data.Integer = token.GetIntegerValue();
                        break;

                    case EValueType::Double:
                        if (token.GetType() != ETokenType::Double) {
                            ThrowIncompatibleType(schemaColumn);
                        }
                        value.Data.Double = token.GetDoubleValue();
                        break;

                    case EValueType::String:
                        if (token.GetType() != ETokenType::String) {
                            ThrowIncompatibleType(schemaColumn);
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

            value.Id = NameTable->GetIdOrRegisterName(pair.first);
            value.Type = EValueType::Any;
            value.Length = pair.second.size();
            value.Data.String = pair.second.begin();
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

void TTableChunkReaderAdapter::ThrowIncompatibleType(const TColumnSchema& schema)
{
    THROW_ERROR_EXCEPTION(
        "Chunk data in column %s is incompatible with schema",
        ~schema.Name.Quote());
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
