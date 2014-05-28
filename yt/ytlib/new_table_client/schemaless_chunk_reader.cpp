#include "stdafx.h"

#include "schemaless_chunk_reader.h"

#include "chunk_reader_base.h"
#include "config.h"
#include "name_table.h"
#include "private.h"
#include "schema.h"
#include "schemaless_block_reader.h"

#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/multi_chunk_reader_base.h>

#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/object_service_proxy.h>

// TKeyColumnsExt
#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/transaction_client/helpers.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/ypath/rich.h>

#include <core/concurrency/scheduler.h>

#include <core/misc/protobuf_helpers.h>

#include <core/ytree/ypath_proxy.h>


namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProto;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

using NChunkClient::TReadLimit;
using NChunkClient::TChannel;
using NRpc::IChannelPtr;
using NTableClient::TTableYPathProxy;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessChunkReader
    : public ISchemalessChunkReader
    , public TChunkReaderBase
{
public:
    TSchemalessChunkReader(
        TChunkReaderConfigPtr config,
        IAsyncReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns,
        const TChunkMeta& masterMeta,
        const TReadLimit& lowerLimit,
        const TReadLimit& upperLimit,
        const TColumnFilter& columnFilter,
        i64 tableRowIndex,
        TNullable<int> partitionTag);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;
    virtual i64 GetTableRowIndex() const override;

private:
    TNameTablePtr NameTable_;
    TNameTablePtr ChunkNameTable_;

    TColumnFilter ColumnFilter_;
    TKeyColumns KeyColumns_;

    const i64 TableRowIndex_;

    TNullable<int> PartitionTag_;

    // Maps chunk name table ids into client id.
    // For filtered out columns maps id to -1.
    std::vector<int> IdMapping_;

    std::unique_ptr<THorizontalSchemalessBlockReader> BlockReader_;

    int CurrentBlockIndex_;
    i64 CurrentRowIndex_;
    i64 RowCount_;

    TChunkMeta ChunkMeta_;
    TBlockMetaExt BlockMetaExt_;

    virtual std::vector<NChunkClient::TSequentialReader::TBlockInfo> GetBlockSequence() override;

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

    std::vector<TSequentialReader::TBlockInfo> CreateBlockSequence(int beginIndex, int endIndex);
    void DownloadChunkMeta(std::vector<int> extensionTags, TNullable<int> partitionTag = Null);

    std::vector<TSequentialReader::TBlockInfo> GetBlockSequencePartition(); 
    std::vector<TSequentialReader::TBlockInfo> GetBlockSequenceSorted();
    std::vector<TSequentialReader::TBlockInfo> GetBlockSequenceUnsorted(); 

};

////////////////////////////////////////////////////////////////////////////////

TSchemalessChunkReader::TSchemalessChunkReader(
    TChunkReaderConfigPtr config,
    IAsyncReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    const TChunkMeta& masterMeta,
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    const TColumnFilter& columnFilter,
    i64 tableRowIndex,
    TNullable<int> partitionTag)
    : TChunkReaderBase(
        config, 
        lowerLimit,
        upperLimit,
        underlyingReader, 
        GetProtoExtension<TMiscExt>(masterMeta.extensions()))
    , NameTable_(nameTable)
    , ChunkNameTable_(New<TNameTable>())
    , ColumnFilter_(columnFilter)
    , KeyColumns_(keyColumns)
    , TableRowIndex_(tableRowIndex)
    , PartitionTag_(partitionTag)
    , ChunkMeta_(masterMeta)
{
    Logger.AddTag(Sprintf("SchemalessChunkReader: %p", this));
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequence() 
{
    YCHECK(ChunkMeta_.version() == ETableChunkFormat::SchemalessHorizontal);

    if (PartitionTag_) {
        return GetBlockSequencePartition();
    }

    bool readSorted = LowerLimit_.HasKey() || UpperLimit_.HasKey() || !KeyColumns_.empty();
    if (readSorted) {
        return GetBlockSequenceSorted();
    } else {
        return GetBlockSequenceUnsorted();
    }
}

void TSchemalessChunkReader::DownloadChunkMeta(std::vector<int> extensionTags, TNullable<int> partitionTag)
{
    extensionTags.push_back(TProtoExtensionTag<TBlockMetaExt>::Value);
    extensionTags.push_back(TProtoExtensionTag<TNameTableExt>::Value);

    auto errorOrMeta = WaitFor(UnderlyingReader_->AsyncGetChunkMeta(partitionTag, &extensionTags));
    THROW_ERROR_EXCEPTION_IF_FAILED(errorOrMeta);

    ChunkMeta_ = errorOrMeta.Value();
    BlockMetaExt_ = GetProtoExtension<TBlockMetaExt>(ChunkMeta_.extensions());

    auto nameTableExt = GetProtoExtension<TNameTableExt>(ChunkMeta_.extensions());
    FromProto(&ChunkNameTable_, nameTableExt);

    IdMapping_.resize(ChunkNameTable_->GetSize(), -1);

    if (ColumnFilter_.All) {
        for (int chunkNameId = 0; chunkNameId < ChunkNameTable_->GetSize(); ++chunkNameId) {
            auto name = ChunkNameTable_->GetName(chunkNameId);
            auto id = NameTable_->GetIdOrRegisterName(name);
            IdMapping_[chunkNameId] = id;
        }
    } else {
        for (auto id : ColumnFilter_.Indexes) {
            auto name = NameTable_->GetName(id);
            auto chunkNameId = ChunkNameTable_->FindId(name);
            if (chunkNameId) {
                IdMapping_[chunkNameId.Get()] = id;
            }

        }
    }
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequenceSorted() 
{
    if (!Misc_.sorted()) {
        THROW_ERROR_EXCEPTION("Requested sorted read for unsorted chunk");
    }

    std::vector<int> extensionTags = {
        TProtoExtensionTag<TBlockIndexExt>::Value,
        TProtoExtensionTag<NTableClient::NProto::TKeyColumnsExt>::Value,
        TProtoExtensionTag<TBoundaryKeysExt>::Value
    };

    DownloadChunkMeta(extensionTags);

    auto keyColumnsExt = GetProtoExtension<NTableClient::NProto::TKeyColumnsExt>(ChunkMeta_.extensions());
    TKeyColumns chunkKeyColumns = NYT::FromProto<TKeyColumns>(keyColumnsExt);

    ValidateKeyColumns(KeyColumns_, chunkKeyColumns);

    if (KeyColumns_.empty()) {
        KeyColumns_ = chunkKeyColumns;
    }

    auto blockIndexExt = GetProtoExtension<TBlockIndexExt>(ChunkMeta_.extensions());
    auto boundaryKeysExt = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_.extensions());

    int beginIndex = std::max(
        GetBeginBlockIndex(BlockMetaExt_),
        GetBeginBlockIndex(blockIndexExt, boundaryKeysExt));
    int endIndex = std::min(GetEndBlockIndex(BlockMetaExt_), GetEndBlockIndex(blockIndexExt));

    return CreateBlockSequence(beginIndex, endIndex);
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequencePartition() 
{
    YCHECK(LowerLimit_.IsTrivial());
    YCHECK(UpperLimit_.IsTrivial());
    YUNIMPLEMENTED();
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::GetBlockSequenceUnsorted() 
{
    DownloadChunkMeta(std::vector<int>());

    return CreateBlockSequence(
        GetBeginBlockIndex(BlockMetaExt_), 
        GetEndBlockIndex(BlockMetaExt_));
}

std::vector<TSequentialReader::TBlockInfo> TSchemalessChunkReader::CreateBlockSequence(int beginIndex, int endIndex)
{
    std::vector<TSequentialReader::TBlockInfo> blocks;

    if (beginIndex >= BlockMetaExt_.entries_size()) {
        return blocks;
    }

    CurrentBlockIndex_ = beginIndex;
    auto& blockMeta = BlockMetaExt_.entries(CurrentBlockIndex_);

    CurrentRowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();
    for (int index = CurrentBlockIndex_; index < endIndex; ++index) {
        TSequentialReader::TBlockInfo blockInfo;
        blockInfo.Index = index;
        blockInfo.Size = BlockMetaExt_.entries(index).block_size();
        blocks.push_back(blockInfo);
    }
    return blocks;
}

void TSchemalessChunkReader::InitFirstBlock()
{
    BlockReader_.reset(new THorizontalSchemalessBlockReader(
        SequentialReader_->GetBlock(),
        BlockMetaExt_.entries(CurrentBlockIndex_),
        IdMapping_,
        KeyColumns_.size()));

    if (LowerLimit_.HasRowIndex()) {
        YCHECK(BlockReader_->SkipToRowIndex(LowerLimit_.GetRowIndex() - CurrentRowIndex_));
        CurrentRowIndex_ = LowerLimit_.GetRowIndex();
    }

    if (LowerLimit_.HasKey()) {
        auto blockRowIndex = BlockReader_->GetRowIndex();
        YCHECK(BlockReader_->SkipToKey(LowerLimit_.GetKey()));
        CurrentRowIndex_ += BlockReader_->GetRowIndex() - blockRowIndex;
    }
}

void TSchemalessChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    BlockReader_.reset(new THorizontalSchemalessBlockReader(
        SequentialReader_->GetBlock(),
        BlockMetaExt_.entries(CurrentBlockIndex_),
        IdMapping_,
        KeyColumns_.size()));
}

bool TSchemalessChunkReader::Read(std::vector<TUnversionedRow>* rows)
{
    YCHECK(rows->capacity() > 0);

    MemoryPool_.Clear();
    rows->clear();

    if (!ReadyEvent_.IsSet()) {
        // Waiting for the next block.
        return true;
    }

    if (!BlockReader_) {
        // Nothing to read from chunk.
        return false;
    }

    if (BlockEnded_) {
        BlockReader_.reset();
        return OnBlockEnded();
    }

    while (rows->size() < rows->capacity()) {
        if (UpperLimit_.HasRowIndex() && CurrentRowIndex_ >= UpperLimit_.GetRowIndex()) {
            return false;
        }

        if (UpperLimit_.HasKey() && CompareRows(BlockReader_->GetKey(), UpperLimit_.GetKey()) >= 0) {
            return false;
        }

        ++RowCount_;
        ++CurrentRowIndex_;
        rows->push_back(BlockReader_->GetRow(&MemoryPool_));
        
        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
            return true;
        }
    }

    return true;
}

i64 TSchemalessChunkReader::GetTableRowIndex() const
{
    return TableRowIndex_ + CurrentRowIndex_;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessChunkReader(
    TChunkReaderConfigPtr config,
    IAsyncReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    const TChunkMeta& masterMeta,
    const TReadLimit& lowerLimit,
    const TReadLimit& upperLimit,
    const TColumnFilter& columnFilter,
    i64 tableRowIndex,
    TNullable<int> partitionTag)
{
    return New<TSchemalessChunkReader>(
        config, 
        underlyingReader, 
        nameTable, 
        keyColumns, 
        masterMeta, 
        lowerLimit, 
        upperLimit, 
        columnFilter,
        tableRowIndex,
        partitionTag);
}

////////////////////////////////////////////////////////////////////////////////

template<class TBase>
class TSchemalessMultiChunkReader
    : public ISchemalessMultiChunkReader
    , public TBase
{
public:
    TSchemalessMultiChunkReader(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        IChannelPtr masterChannel,
        IBlockCachePtr blockCache,
        TNodeDirectoryPtr nodeDirectory,
        const std::vector<TChunkSpec>& chunkSpecs,
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns);

    virtual bool Read(std::vector<TUnversionedRow>* rows) override;

    virtual i64 GetTableRowIndex() const override;

    virtual int GetTableIndex() const override;

private:
    TMultiChunkReaderConfigPtr Config_;
    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;

    ISchemalessChunkReaderPtr CurrentReader_;

    using TBase::ReadyEvent_;
    using TBase::CurrentSession_;
    using TBase::ChunkSpecs_;

    virtual IChunkReaderBasePtr CreateTemplateReader(const TChunkSpec& chunkSpec, IAsyncReaderPtr asyncReader) override;
    virtual void OnReaderSwitched() override;

};

////////////////////////////////////////////////////////////////////////////////

template<class TBase>
TSchemalessMultiChunkReader<TBase>::TSchemalessMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IChannelPtr masterChannel,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns)
    : TBase(config, options, masterChannel, blockCache, nodeDirectory, chunkSpecs)
    , Config_(config)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
{ }

template <class TBase>
bool TSchemalessMultiChunkReader<TBase>::Read(std::vector<TUnversionedRow>* rows)
{
    YCHECK(ReadyEvent_.IsSet());
    YCHECK(ReadyEvent_.Get().IsOK());

    // Nothing to read.
    if (!CurrentReader_)
        return false;

    bool readerFinished = !CurrentReader_->Read(rows);
    if (rows->empty()) {
        return TBase::OnEmptyRead(readerFinished);
    } else {
        return true;
    }
}

template <class TBase>
IChunkReaderBasePtr TSchemalessMultiChunkReader<TBase>::CreateTemplateReader(
    const TChunkSpec& chunkSpec,
    IAsyncReaderPtr asyncReader)
{
    using NYT::FromProto;

    auto channel = chunkSpec.has_channel()
            ? FromProto<TChannel>(chunkSpec.channel())
            : TChannel::Universal();

    return CreateSchemalessChunkReader(
        Config_, 
        asyncReader, 
        NameTable_,
        KeyColumns_, 
        chunkSpec.chunk_meta(),
        chunkSpec.has_lower_limit() ? TReadLimit(chunkSpec.lower_limit()) : TReadLimit(),
        chunkSpec.has_upper_limit() ? TReadLimit(chunkSpec.upper_limit()) : TReadLimit(),
        CreateColumnFilter(channel, NameTable_),
        chunkSpec.table_row_index(),
        chunkSpec.has_partition_tag() ? MakeNullable(chunkSpec.partition_tag()) : Null);
}

template <class TBase>
void TSchemalessMultiChunkReader<TBase>::OnReaderSwitched()
{
    CurrentReader_ = dynamic_cast<ISchemalessChunkReader*>(CurrentSession_.ChunkReader.Get());
    YCHECK(CurrentReader_);
}

template <class TBase>
i64 TSchemalessMultiChunkReader<TBase>::GetTableRowIndex() const
{
    return CurrentReader_ ? CurrentReader_->GetTableRowIndex() : 0;
}

template <class TBase>
int TSchemalessMultiChunkReader<TBase>::GetTableIndex() const
{
    return ChunkSpecs_[CurrentSession_.ChunkSpecIndex].table_index();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSequentialMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IChannelPtr masterChannel,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns)
{
    return New<TSchemalessMultiChunkReader<TSequentialMultiChunkReaderBase>>(
        config,
        options,
        masterChannel,
        blockCache,
        nodeDirectory,
        chunkSpecs,
        nameTable,
        keyColumns);
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessParallelMultiChunkReader(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    IChannelPtr masterChannel,
    IBlockCachePtr blockCache,
    TNodeDirectoryPtr nodeDirectory,
    const std::vector<TChunkSpec>& chunkSpecs,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns)
{
    return New<TSchemalessMultiChunkReader<TParallelMultiChunkReaderBase>>(
        config,
        options,
        masterChannel,
        blockCache,
        nodeDirectory,
        chunkSpecs,
        nameTable,
        keyColumns);
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessTableReader
    : public ISchemalessTableReader
    , public TTransactionListener
{
public:
    TSchemalessTableReader(
        TTableReaderConfigPtr config,
        IChannelPtr masterChannel,
        TTransactionPtr transaction,
        IBlockCachePtr blockCache,
        const TRichYPath& richPath,
        TNameTablePtr nameTable);

    virtual TAsyncError Open() override;
    virtual bool Read(std::vector<TUnversionedRow>* rows) override;
    virtual TAsyncError GetReadyEvent() override;

    virtual i64 GetTableRowIndex() const override;


private:
    NLog::TTaggedLogger Logger;

    TTableReaderConfigPtr Config_;
    IChannelPtr MasterChannel_;
    TTransactionPtr Transaction_;
    IBlockCachePtr BlockCache_;
    TRichYPath RichPath_;
    TNameTablePtr NameTable_;

    TTransactionId TransactionId_;

    ISchemalessMultiChunkReaderPtr UnderlyingReader_;

    TError DoOpen();

};

////////////////////////////////////////////////////////////////////////////////

TSchemalessTableReader::TSchemalessTableReader(
    TTableReaderConfigPtr config,
    IChannelPtr masterChannel,
    TTransactionPtr transaction,
    IBlockCachePtr blockCache,
    const TRichYPath& richPath,
    TNameTablePtr nameTable)
    : Logger(TableReaderLogger)
    , Config_(config)
    , MasterChannel_(masterChannel)
    , Transaction_(transaction)
    , BlockCache_(blockCache)
    , RichPath_(richPath)
    , NameTable_(nameTable)
    , TransactionId_(transaction ? transaction->GetId() : NullTransactionId)
{
    YCHECK(masterChannel);

    Logger.AddTag(Sprintf("Path: %s, TransactihonId: %s",
        ~RichPath_.GetPath(),
        ~ToString(TransactionId_)));
}

TAsyncError TSchemalessTableReader::Open()
{
    LOG_INFO("Opening table reader");

    return BIND(&TSchemalessTableReader::DoOpen, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

TError TSchemalessTableReader::DoOpen()
{
    TObjectServiceProxy objectProxy(MasterChannel_);

    const auto& path = RichPath_.GetPath();
    auto batchReq = objectProxy.ExecuteBatch();

    {
        auto req = TYPathProxy::Get(path + "/@type");
        SetTransactionId(req, Transaction_);
        SetSuppressAccessTracking(req, Config_->SuppressAccessTracking);
        batchReq->AddRequest(req, "get_type");
    } {
        auto req = TTableYPathProxy::Fetch(path);
        InitializeFetchRequest(req.Get(), RichPath_);
        req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        SetTransactionId(req, Transaction_);
        SetSuppressAccessTracking(req, Config_->SuppressAccessTracking);
        batchReq->AddRequest(req, "fetch");
    }

    LOG_INFO("Fetching table info");
    auto batchRsp = WaitFor(batchReq->Invoke());
    if (!batchRsp->IsOK()) {
        return TError("Error fetching table info") << *batchRsp;
    }

    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_type");
        if (!rsp->IsOK()) {
            return TError("Error getting object type") << *rsp;
        }

        auto type = ConvertTo<EObjectType>(TYsonString(rsp->value()));
        if (type != EObjectType::Table) {
            return TError("Invalid type of %s: expected %s, actual %s",
                ~RichPath_.GetPath(),
                ~FormatEnum(EObjectType(EObjectType::Table)).Quote(),
                ~FormatEnum(type).Quote());
        }
    } {
        auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspFetch>("fetch");
        if (!rsp->IsOK()) {
            return TError("Error fetching table chunks") << *rsp;
        }

        auto nodeDirectory = New<TNodeDirectory>();
        nodeDirectory->MergeFrom(rsp->node_directory());

        std::vector<TChunkSpec> chunkSpecs;
        for (const auto& chunkSpec : rsp->chunks()) {
            if (!IsUnavailable(chunkSpec)) {
                chunkSpecs.push_back(chunkSpec);
                continue;
            }
             
            if (Config_->IgnoreUnavailableChunks) {
                continue;
            }
             
            return TError(
                "Chunk is unavailable (ChunkId: %s)",
                ~ToString(NYT::FromProto<TChunkId>(chunkSpec.chunk_id())));
        }

        UnderlyingReader_ = CreateSchemalessSequentialMultiChunkReader(
            Config_,
            New<TMultiChunkReaderOptions>(),
            MasterChannel_,
            BlockCache_,
            nodeDirectory,
            chunkSpecs,
            NameTable_);

        auto error = WaitFor(UnderlyingReader_->Open());
        RETURN_IF_ERROR(error);
    }

    if (Transaction_) {
        ListenTransaction(Transaction_);
    }

    LOG_INFO("Table reader opened");
    return TError();
}

bool TSchemalessTableReader::Read(std::vector<TUnversionedRow> *rows)
{
    if (IsAborted()) {
        return true;
    }

    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->Read(rows);
}

TAsyncError TSchemalessTableReader::GetReadyEvent()
{
    if (IsAborted()) {
        return MakeFuture(TError("Transaction aborted (TransactionId: %s))", ~ToString(TransactionId_)));
    }

    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetReadyEvent();
}

i64 TSchemalessTableReader::GetTableRowIndex() const
{
    YCHECK(UnderlyingReader_);
    return UnderlyingReader_->GetTableRowIndex();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessTableReaderPtr CreateSchemalessTableReader(
    TTableReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NTransactionClient::TTransactionPtr transaction,
    NChunkClient::IBlockCachePtr blockCache,
    const NYPath::TRichYPath& richPath,
    TNameTablePtr nameTable)
{
    return New<TSchemalessTableReader>(
        config,
        masterChannel,
        transaction,
        blockCache,
        richPath,
        nameTable);
}


/*
IPartitionChunkReaderPtr CreatePartitionChunkReader()
{
    // How does it look like.
}
*/

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
