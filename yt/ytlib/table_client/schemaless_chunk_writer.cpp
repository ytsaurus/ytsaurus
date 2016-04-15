#include "schemaless_chunk_writer.h"
#include "chunk_meta_extensions.h"
#include "chunk_writer_base.h"
#include "config.h"
#include "name_table.h"
#include "partitioner.h"
#include "schemaless_block_writer.h"
#include "schemaless_row_reorderer.h"
#include "table_ypath_proxy.h"

#include <yt/ytlib/api/client.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/encoding_chunk_writer.h>
#include <yt/ytlib/chunk_client/multi_chunk_writer_base.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/transaction_client/transaction_listener.h>
#include <yt/ytlib/transaction_client/config.h>

#include <yt/ytlib/api/transaction.h>
#include <yt/ytlib/api/config.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/channel.h>

#include <yt/core/ytree/helpers.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTableClient::NProto;
using namespace NRpc;
using namespace NApi;
using namespace NTransactionClient;
using namespace NNodeTrackerClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NApi;

using NYT::ToProto;
using NYT::FromProto;


static const i64 PartitionRowCountThreshold = (i64)100 * 1000;
static const i64 PartitionRowCountLimit = std::numeric_limits<i32>::max() - PartitionRowCountThreshold;

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
class TSchemalessChunkWriter
    : public TBase
    , public ISchemalessChunkWriter
{
public:
    TSchemalessChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        TNameTablePtr nameTable,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns = TKeyColumns());

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override;

    virtual TNameTablePtr GetNameTable() const override;

    virtual bool IsSorted() const override;

private:
    const TNameTablePtr NameTable_;

    THorizontalSchemalessBlockWriter* CurrentBlockWriter_;

    virtual ETableChunkFormat GetFormatVersion() const override;
    virtual IBlockWriter* CreateBlockWriter() override;

    virtual void PrepareChunkMeta() override;

    virtual i64 GetMetaSize() const override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
TSchemalessChunkWriter<TBase>::TSchemalessChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TNameTablePtr nameTable,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns)
    : TBase(
        config,
        options,
        chunkWriter,
        blockCache,
        keyColumns)
    , NameTable_(nameTable)
{ }

template <class TBase>
bool TSchemalessChunkWriter<TBase>::Write(const std::vector<TUnversionedRow>& rows)
{
    YCHECK(CurrentBlockWriter_);

    for (auto row : rows) {
        this->ValidateDuplicateIds(row, NameTable_);
        CurrentBlockWriter_->WriteRow(row);
        this->OnRow(row);
    }

    return TBase::EncodingChunkWriter_->IsReady();
}

template <class TBase>
ETableChunkFormat TSchemalessChunkWriter<TBase>::GetFormatVersion() const
{
    return ETableChunkFormat::SchemalessHorizontal;
}

template <class TBase>
void TSchemalessChunkWriter<TBase>::PrepareChunkMeta()
{
    TBase::PrepareChunkMeta();

    auto& meta = TBase::EncodingChunkWriter_->Meta();
    TNameTableExt nameTableExt;
    ToProto(&nameTableExt, NameTable_);

    SetProtoExtension(meta.mutable_extensions(), nameTableExt);
}

template <class TBase>
IBlockWriter* TSchemalessChunkWriter<TBase>::CreateBlockWriter()
{
    CurrentBlockWriter_ = new THorizontalSchemalessBlockWriter();
    return CurrentBlockWriter_;
}

template <class TBase>
TNameTablePtr TSchemalessChunkWriter<TBase>::GetNameTable() const
{
    return NameTable_;
}

template <class TBase>
bool TSchemalessChunkWriter<TBase>::IsSorted() const
{
    return TBase::IsSorted();
}

template <class TBase>
i64 TSchemalessChunkWriter<TBase>::GetMetaSize() const
{
    return NameTable_->GetByteSize() + TBase::GetMetaSize();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkWriterPtr CreateSchemalessChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache)
{
    if (keyColumns.empty()) {
        return New<TSchemalessChunkWriter<TSequentialChunkWriterBase>>(
            config,
            options,
            nameTable,
            chunkWriter,
            blockCache);
    } else {
        return New<TSchemalessChunkWriter<TSortedChunkWriterBase>>(
            config,
            options,
            nameTable,
            chunkWriter,
            blockCache,
            keyColumns);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TPartitionChunkWriter
    : public ISchemalessChunkWriter
    , public TChunkWriterBase
{
public:
    TPartitionChunkWriter(
        TChunkWriterConfigPtr config,
        TChunkWriterOptionsPtr options,
        TNameTablePtr nameTable,
        IChunkWriterPtr chunkWriter,
        IBlockCachePtr blockCache,
        const TKeyColumns& keyColumns,
        IPartitioner* partitioner);

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override;

    virtual TNameTablePtr GetNameTable() const override;

    virtual i64 GetDataSize() const override;

    virtual TChunkMeta GetSchedulerMeta() const override;

    virtual bool IsCloseDemanded() const override;

    virtual i64 GetMetaSize() const override;

    virtual bool IsSorted() const override;

private:
    const TNameTablePtr NameTable_;
    const TKeyColumns KeyColumns_;

    TPartitionsExt PartitionsExt_;

    IPartitioner* Partitioner_;

    std::vector<std::unique_ptr<THorizontalSchemalessBlockWriter>> BlockWriters_;

    i64 CurrentBufferCapacity_ = 0;

    int LargestPartitionIndex_ = 0;
    i64 LargestPartitionSize_ = 0;

    i64 LargestPartitionRowCount_ = 0;

    i64 BlockReserveSize_;

    i64 FlushedRowCount_ = 0;


    void WriteRow(TUnversionedRow row);

    void InitLargestPartition();
    void FlushBlock(int partitionIndex);

    virtual void DoClose() override;
    virtual void PrepareChunkMeta() override;

    virtual ETableChunkFormat GetFormatVersion() const override;

};

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkWriter::TPartitionChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TNameTablePtr nameTable,
    IChunkWriterPtr chunkWriter,
    IBlockCachePtr blockCache,
    const TKeyColumns& keyColumns,
    IPartitioner* partitioner)
    : TChunkWriterBase(
        config,
        options,
        chunkWriter,
        blockCache)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , Partitioner_(partitioner)
{
    int partitionCount = Partitioner_->GetPartitionCount();
    BlockWriters_.reserve(partitionCount);

    BlockReserveSize_ = Config_->MaxBufferSize / partitionCount;

    for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
        BlockWriters_.emplace_back(new THorizontalSchemalessBlockWriter(BlockReserveSize_));
        CurrentBufferCapacity_ += BlockWriters_.back()->GetCapacity();

        auto* partitionAttributes = PartitionsExt_.add_partitions();
        partitionAttributes->set_row_count(0);
        partitionAttributes->set_uncompressed_data_size(0);
    }
}

bool TPartitionChunkWriter::Write(const std::vector<TUnversionedRow>& rows)
{
    for (auto row : rows) {
        this->ValidateDuplicateIds(row, NameTable_);
        WriteRow(row);
    }

    return EncodingChunkWriter_->IsReady();
}

void TPartitionChunkWriter::WriteRow(TUnversionedRow row)
{
    ++RowCount_;

    i64 weight = GetDataWeight(row);
    ValidateRowWeight(weight);
    DataWeight_ += weight;

    auto partitionIndex = Partitioner_->GetPartitionIndex(row);
    auto& blockWriter = BlockWriters_[partitionIndex];

    CurrentBufferCapacity_ -= blockWriter->GetCapacity();
    i64 oldSize = blockWriter->GetBlockSize();

    blockWriter->WriteRow(row);

    CurrentBufferCapacity_ += blockWriter->GetCapacity();
    i64 newSize = blockWriter->GetBlockSize();

    auto* partitionAttributes = PartitionsExt_.mutable_partitions(partitionIndex);
    partitionAttributes->set_row_count(partitionAttributes->row_count() + 1);
    partitionAttributes->set_uncompressed_data_size(partitionAttributes->uncompressed_data_size() + newSize - oldSize);

    LargestPartitionRowCount_ = std::max(
        partitionAttributes->row_count(), 
        LargestPartitionRowCount_);

    if (newSize > LargestPartitionSize_) {
        LargestPartitionIndex_ = partitionIndex;
        LargestPartitionSize_ = newSize;
    }

    if (LargestPartitionSize_ >= Config_->BlockSize || CurrentBufferCapacity_ >= Config_->MaxBufferSize) {
        CurrentBufferCapacity_ -= BlockWriters_[LargestPartitionIndex_]->GetCapacity();

        FlushBlock(LargestPartitionIndex_);
        BlockWriters_[LargestPartitionIndex_].reset(new THorizontalSchemalessBlockWriter(BlockReserveSize_));
        CurrentBufferCapacity_ += BlockWriters_[LargestPartitionIndex_]->GetCapacity();

        InitLargestPartition();
    }
}

bool TPartitionChunkWriter::IsCloseDemanded() const
{
    return LargestPartitionRowCount_ > PartitionRowCountLimit;
}

void TPartitionChunkWriter::FlushBlock(int partitionIndex)
{
    auto& blockWriter = BlockWriters_[partitionIndex];
    auto block = blockWriter->FlushBlock();
    block.Meta.set_partition_index(partitionIndex);
    FlushedRowCount_ += block.Meta.row_count();
    block.Meta.set_chunk_row_count(FlushedRowCount_);

    RegisterBlock(block);
}

void TPartitionChunkWriter::InitLargestPartition()
{
    LargestPartitionIndex_ = 0;
    LargestPartitionSize_ = BlockWriters_.front()->GetBlockSize();
    for (int partitionIndex = 1; partitionIndex < BlockWriters_.size(); ++partitionIndex) {
        auto& blockWriter = BlockWriters_[partitionIndex];
        if (blockWriter->GetBlockSize() > LargestPartitionSize_) {
            LargestPartitionSize_ = blockWriter->GetBlockSize();
            LargestPartitionIndex_ = partitionIndex;
        }
    }
}

i64 TPartitionChunkWriter::GetDataSize() const
{
    return TChunkWriterBase::GetDataSize() + CurrentBufferCapacity_;
}

TChunkMeta TPartitionChunkWriter::GetSchedulerMeta() const
{
    auto meta = TChunkWriterBase::GetSchedulerMeta();
    SetProtoExtension(meta.mutable_extensions(), PartitionsExt_);
    return meta;
}

i64 TPartitionChunkWriter::GetMetaSize() const
{
    return TChunkWriterBase::GetMetaSize() + 2 * sizeof(i64) * BlockWriters_.size();
}

TNameTablePtr TPartitionChunkWriter::GetNameTable() const
{
    return NameTable_;
}

void TPartitionChunkWriter::DoClose()
{
    for (int partitionIndex = 0; partitionIndex < BlockWriters_.size(); ++partitionIndex) {
        if (BlockWriters_[partitionIndex]->GetRowCount() > 0) {
            FlushBlock(partitionIndex);
        }
    }

    TChunkWriterBase::DoClose();
}

void TPartitionChunkWriter::PrepareChunkMeta()
{
    TChunkWriterBase::PrepareChunkMeta();

    LOG_DEBUG("Partition totals: %v", PartitionsExt_.DebugString());

    auto& meta = EncodingChunkWriter_->Meta();

    SetProtoExtension(meta.mutable_extensions(), PartitionsExt_);

    TKeyColumnsExt keyColumnsExt;
    ToProto(keyColumnsExt.mutable_names(), KeyColumns_);
    SetProtoExtension(meta.mutable_extensions(), keyColumnsExt);

    TNameTableExt nameTableExt;
    ToProto(&nameTableExt, NameTable_);
    SetProtoExtension(meta.mutable_extensions(), nameTableExt);
}

ETableChunkFormat TPartitionChunkWriter::GetFormatVersion() const
{
    return ETableChunkFormat::SchemalessHorizontal;
}

bool TPartitionChunkWriter::IsSorted() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkWriterPtr CreatePartitionChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IChunkWriterPtr chunkWriter,
    IPartitioner* partitioner,
    IBlockCachePtr blockCache)
{
    return New<TPartitionChunkWriter>(
        config,
        options,
        nameTable,
        chunkWriter,
        blockCache,
        keyColumns,
        partitioner);
}

////////////////////////////////////////////////////////////////////////////////

struct TReorderingSchemalessWriterPoolTag { };

class TReorderingSchemalessMultiChunkWriter
    : public ISchemalessMultiChunkWriter
{
public:
    TReorderingSchemalessMultiChunkWriter(
        const TKeyColumns& keyColumns,
        TNameTablePtr nameTable,
        TOwningKey lastKey,
        ISchemalessMultiChunkWriterPtr underlyingWriter);

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override;

    virtual TNameTablePtr GetNameTable() const override;

    virtual bool IsSorted() const override;

private:
    TChunkedMemoryPool MemoryPool_;
    TSchemalessRowReorderer RowReorderer_;
    ISchemalessMultiChunkWriterPtr UnderlyingWriter_;

    TOwningKey LastKey_;
    int KeyColumnCount_;
    TError Error_;


    bool CheckSortOrder(TUnversionedRow lhs, TUnversionedRow rhs);

    virtual TFuture<void> Open() override;
    virtual TFuture<void> GetReadyEvent() override;
    virtual TFuture<void> Close() override;

    virtual void SetProgress(double progress) override;
    virtual const std::vector<TChunkSpec>& GetWrittenChunksMasterMeta() const override;
    virtual const std::vector<TChunkSpec>& GetWrittenChunksFullMeta() const override;
    virtual TNodeDirectoryPtr GetNodeDirectory() const override;
    virtual TDataStatistics GetDataStatistics() const override;

};

////////////////////////////////////////////////////////////////////////////////

TReorderingSchemalessMultiChunkWriter::TReorderingSchemalessMultiChunkWriter(
    const TKeyColumns& keyColumns,
    TNameTablePtr nameTable,
    TOwningKey lastKey,
    ISchemalessMultiChunkWriterPtr underlyingWriter)
    : MemoryPool_(TReorderingSchemalessWriterPoolTag())
    , RowReorderer_(nameTable, keyColumns)
    , UnderlyingWriter_(underlyingWriter)
    , LastKey_(lastKey)
    , KeyColumnCount_(keyColumns.size())
{ }

bool TReorderingSchemalessMultiChunkWriter::CheckSortOrder(TUnversionedRow lhs, TUnversionedRow rhs)
{
    try {
        if (CompareRows(lhs, rhs, KeyColumnCount_) <= 0) {
            return true;
        }
        TUnversionedOwningRowBuilder leftBuilder, rightBuilder;
        for (int i = 0; i < KeyColumnCount_; ++i) {
            leftBuilder.AddValue(lhs[i]);
            rightBuilder.AddValue(rhs[i]);
        }

        Error_ = TError(
            EErrorCode::SortOrderViolation,
            "Sort order violation: %v > %v",
            leftBuilder.FinishRow(),
            rightBuilder.FinishRow());
    } catch (const std::exception& ex) {
        // NB: e.g. incomparable type.
        Error_ = TError(ex);
    }
    return false;
}

bool TReorderingSchemalessMultiChunkWriter::Write(const std::vector<TUnversionedRow>& rows)
{
    std::vector<TUnversionedRow> reorderedRows;
    reorderedRows.reserve(rows.size());

    for (const auto& row : rows) {
        reorderedRows.push_back(RowReorderer_.ReorderRow(row, &MemoryPool_));
    }

    if (IsSorted() && !reorderedRows.empty()) {
        if (!CheckSortOrder(LastKey_, reorderedRows.front())) {
            return false;
        }

        for (int i = 1; i < reorderedRows.size(); ++i) {
            if (!CheckSortOrder(reorderedRows[i-1], reorderedRows[i])) {
              return false;
            }
        }

        const auto& lastKey = reorderedRows.back();
        TUnversionedOwningRowBuilder keyBuilder;
        for (int i = 0; i < KeyColumnCount_; ++i) {
            keyBuilder.AddValue(lastKey[i]);
        }
        LastKey_ = keyBuilder.FinishRow();
    }

    auto result = UnderlyingWriter_->Write(reorderedRows);
    MemoryPool_.Clear();

    return result;
}

TFuture<void> TReorderingSchemalessMultiChunkWriter::Open()
{
    return UnderlyingWriter_->Open();
}

TFuture<void> TReorderingSchemalessMultiChunkWriter::GetReadyEvent()
{
    if (Error_.IsOK()) {
        return UnderlyingWriter_->GetReadyEvent();
    } else {
        return MakeFuture(Error_);
    }
}

TFuture<void> TReorderingSchemalessMultiChunkWriter::Close()
{
    return UnderlyingWriter_->Close();
}

void TReorderingSchemalessMultiChunkWriter::SetProgress(double progress)
{
    UnderlyingWriter_->SetProgress(progress);
}

const std::vector<TChunkSpec>& TReorderingSchemalessMultiChunkWriter::GetWrittenChunksMasterMeta() const
{
    return UnderlyingWriter_->GetWrittenChunksMasterMeta();
}

const std::vector<TChunkSpec>& TReorderingSchemalessMultiChunkWriter::GetWrittenChunksFullMeta() const
{
    return GetWrittenChunksMasterMeta();
}

TNodeDirectoryPtr TReorderingSchemalessMultiChunkWriter::GetNodeDirectory() const
{
    return UnderlyingWriter_->GetNodeDirectory();
}

TDataStatistics TReorderingSchemalessMultiChunkWriter::GetDataStatistics() const
{
    return UnderlyingWriter_->GetDataStatistics();
}

TNameTablePtr TReorderingSchemalessMultiChunkWriter::GetNameTable() const
{
    return UnderlyingWriter_->GetNameTable();
}

bool TReorderingSchemalessMultiChunkWriter::IsSorted() const
{
    return UnderlyingWriter_->IsSorted();
}

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
class TSchemalessMultiChunkWriter
    : public TBase
{
public:
    TSchemalessMultiChunkWriter(
        TMultiChunkWriterConfigPtr config,
        TMultiChunkWriterOptionsPtr options,
        IClientPtr client,
        TCellTag cellTag,
        const TTransactionId& transactionId,
        const TChunkListId& parentChunkListId,
        std::function<ISchemalessChunkWriterPtr(IChunkWriterPtr)> createChunkWriter,
        TNameTablePtr nameTable,
        bool isSorted,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache)
        : TBase(
            config,
            options,
            client,
            cellTag,
            transactionId,
            parentChunkListId,
            createChunkWriter,
            throttler,
            blockCache)
        , NameTable_(nameTable)
        , IsSorted_(isSorted)
    { }

    virtual TNameTablePtr GetNameTable() const override
    {
        return NameTable_;
    }

    virtual bool IsSorted() const override
    {
        return IsSorted_;
    }

private:
    const TNameTablePtr NameTable_;
    const bool IsSorted_;

};

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreateSchemalessMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    TOwningKey lastKey,
    IClientPtr client,
    TCellTag cellTag,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    bool reorderValues,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    typedef TMultiChunkWriterBase<
        ISchemalessMultiChunkWriter,
        ISchemalessChunkWriter,
        const std::vector<TUnversionedRow>&> TSchemalessMultiChunkWriterBase;
    typedef TSchemalessMultiChunkWriter<TSchemalessMultiChunkWriterBase> TWriter;

    auto createChunkWriter = [=] (IChunkWriterPtr underlyingWriter) {
        return CreateSchemalessChunkWriter(
            config,
            options,
            nameTable,
            keyColumns,
            underlyingWriter,
            blockCache);
    };

    bool isSorted = !keyColumns.empty();
    auto writer = New<TWriter>(
        config,
        options,
        client,
        cellTag,
        transactionId,
        parentChunkListId,
        createChunkWriter,
        nameTable,
        isSorted,
        throttler,
        blockCache);

    if (reorderValues && isSorted) {
        return New<TReorderingSchemalessMultiChunkWriter>(
            keyColumns,
            nameTable,
            lastKey,
            writer);
    } else {
        return writer;
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreatePartitionMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IClientPtr client,
    TCellTag cellTag,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    std::unique_ptr<IPartitioner> partitioner,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    YCHECK(!keyColumns.empty());

    typedef TMultiChunkWriterBase<
        ISchemalessMultiChunkWriter,
        ISchemalessChunkWriter,
        const std::vector<TUnversionedRow>&> TPartitionMultiChunkWriterBase;

    typedef TSchemalessMultiChunkWriter<TPartitionMultiChunkWriterBase> TWriter;

    // TODO(babenko): consider making IPartitioner ref-counted.
    auto createChunkWriter = [=, partitioner = std::shared_ptr<IPartitioner>(std::move(partitioner))] (IChunkWriterPtr underlyingWriter) {
        return CreatePartitionChunkWriter(
            config,
            options,
            nameTable,
            keyColumns,
            underlyingWriter,
            partitioner.get(),
            blockCache);
    };

    auto writer = New<TWriter>(
        config,
        options,
        client,
        cellTag,
        transactionId,
        parentChunkListId,
        createChunkWriter,
        nameTable,
        false,
        throttler,
        blockCache);

    return New<TReorderingSchemalessMultiChunkWriter>(
        keyColumns,
        nameTable,
        TOwningKey(),
        writer);
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessTableWriter
    : public ISchemalessWriter
    , public TTransactionListener
{
public:
    TSchemalessTableWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        const TRichYPath& richPath,
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns,
        IClientPtr client,
        ITransactionPtr transaction,
        IThroughputThrottlerPtr throttler,
        IBlockCachePtr blockCache);

    virtual TFuture<void> Open() override;
    virtual bool Write(const std::vector<TUnversionedRow>& rows) override;
    virtual TFuture<void> GetReadyEvent() override;
    virtual TFuture<void> Close() override;
    virtual TNameTablePtr GetNameTable() const override;
    virtual bool IsSorted() const override;

private:
    NLogging::TLogger Logger;

    const TTableWriterConfigPtr Config_;
    const TTableWriterOptionsPtr Options_;
    const TRichYPath RichPath_;
    const TNameTablePtr NameTable_;
    const TKeyColumns KeyColumns_;
    const IClientPtr Client_;
    const ITransactionPtr Transaction_;
    const IThroughputThrottlerPtr Throttler_;
    const IBlockCachePtr BlockCache_;

    TTransactionId TransactionId_;

    TCellTag CellTag_ = InvalidCellTag;
    TObjectId ObjectId_;

    ITransactionPtr UploadTransaction_;
    TChunkListId ChunkListId_;

    TOwningKey LastKey_;

    ISchemalessMultiChunkWriterPtr UnderlyingWriter_;


    void DoOpen();
    void DoClose();

};

////////////////////////////////////////////////////////////////////////////////

TSchemalessTableWriter::TSchemalessTableWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const TRichYPath& richPath,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IClientPtr client,
    ITransactionPtr transaction,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
    : Logger(TableClientLogger)
    , Config_(config)
    , Options_(options)
    , RichPath_(richPath)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , Client_(client)
    , Transaction_(transaction)
    , Throttler_(throttler)
    , BlockCache_(blockCache)
    , TransactionId_(transaction ? transaction->GetId() : NullTransactionId)
{
    if (Transaction_) {
        ListenTransaction(Transaction_);
    }

    Logger.AddTag("Path: %v, TransactionId: %v",
        RichPath_.GetPath(),
        TransactionId_);
}

TFuture<void> TSchemalessTableWriter::Open()
{
    return BIND(&TSchemalessTableWriter::DoOpen, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

bool TSchemalessTableWriter::Write(const std::vector<TUnversionedRow>& rows)
{
    YCHECK(UnderlyingWriter_);
    if (IsAborted()) {
        return false;
    }

    return UnderlyingWriter_->Write(rows);
}

TFuture<void> TSchemalessTableWriter::GetReadyEvent()
{
    if (IsAborted()) {
        return MakeFuture(TError("Transaction %v aborted",
            TransactionId_));
    }

    return UnderlyingWriter_->GetReadyEvent();
}

TFuture<void> TSchemalessTableWriter::Close()
{
    return BIND(&TSchemalessTableWriter::DoClose, MakeStrong(this))
        .AsyncVia(NChunkClient::TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

void TSchemalessTableWriter::DoOpen()
{
    const auto& path = RichPath_.GetPath();
    bool append = RichPath_.GetAppend();
    bool sorted = !KeyColumns_.empty();

    TUserObject userObject;
    userObject.Path = path;

    GetUserObjectBasicAttributes(
        Client_, 
        TMutableRange<TUserObject>(&userObject, 1),
        Transaction_ ? Transaction_->GetId() : NullTransactionId,
        Logger,
        EPermission::Write);

    ObjectId_ = userObject.ObjectId;
    CellTag_ = userObject.CellTag;

    if (userObject.Type != EObjectType::Table) {
        THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
            path,
            EObjectType::Table,
            userObject.Type);
    }

    auto uploadMasterChannel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CellTag_);
    auto objectIdPath = FromObjectId(ObjectId_);

    {
        LOG_INFO("Requesting extended table attributes");

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::LeaderOrFollower);
        TObjectServiceProxy proxy(channel);

        auto req = TCypressYPathProxy::Get(objectIdPath);
        SetTransactionId(req, UploadTransaction_);
        std::vector<Stroka> attributeKeys{
            "replication_factor",
            "compression_codec",
            "erasure_codec",
            "account",
            "vital"
        };
        if (sorted) {
            attributeKeys.push_back("row_count");
            attributeKeys.push_back("sorted_by");
        }
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error requesting extended attributes of table %v",
            path);

        const auto& rsp = rspOrError.Value();
        auto node = ConvertToNode(TYsonString(rsp->value()));
        const auto& attributes = node->Attributes();

        if (append && sorted && attributes.Get<i64>("row_count") > 0) {
            auto tableKeyColumns = attributes.Get<TKeyColumns>("sorted_by", TKeyColumns());

            bool areKeyColumnsCompatible = true;
            if (tableKeyColumns.size() < KeyColumns_.size()) {
                areKeyColumnsCompatible = false;
            } else {
                for (int i = 0; i < KeyColumns_.size(); ++i) {
                    if (tableKeyColumns[i] != KeyColumns_[i]) {
                        areKeyColumnsCompatible = false;
                        break;
                    }
                }
            }

            if (!areKeyColumnsCompatible) {
                THROW_ERROR_EXCEPTION("Key columns mismatch while trying to append sorted data into a non-empty table %v",
                    path)
                    << TErrorAttribute("append_key_columns", KeyColumns_)
                    << TErrorAttribute("current_key_columns", tableKeyColumns);
            }
        }

        Options_->ReplicationFactor = attributes.Get<int>("replication_factor");
        Options_->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
        Options_->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec");
        Options_->Account = attributes.Get<Stroka>("account");
        Options_->ChunksVital = attributes.Get<bool>("vital");

        LOG_INFO("Extended attributes received (Account: %v, CompressionCodec: %v, ErasureCodec: %v)",
            Options_->Account,
            Options_->CompressionCodec,
            Options_->ErasureCodec);
    }

    {
        LOG_INFO("Starting table upload");

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
        TObjectServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TTableYPathProxy::BeginUpload(objectIdPath);
            req->set_update_mode(static_cast<int>(append ? EUpdateMode::Append : EUpdateMode::Overwrite));
            req->set_lock_mode(static_cast<int>((append && !sorted) ? ELockMode::Shared : ELockMode::Exclusive));
            req->set_upload_transaction_title(Format("Upload to %v", path));
            req->set_upload_transaction_timeout(ToProto(Client_->GetConnection()->GetConfig()->TransactionManager->DefaultTransactionTimeout));
            SetTransactionId(req, Transaction_);
            GenerateMutationId(req);
            batchReq->AddRequest(req, "begin_upload");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            "Error starting upload to table %v",
            path);
        const auto& batchRsp = batchRspOrError.Value();

        {
            auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspBeginUpload>("begin_upload").Value();
            auto uploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());

            TTransactionAttachOptions options;
            options.AutoAbort = true;

            UploadTransaction_ = Client_->AttachTransaction(uploadTransactionId, options);
            ListenTransaction(UploadTransaction_);

            LOG_INFO("Table upload started (UploadTransactionId: %v)",
                uploadTransactionId);
        }
    }

    {
        LOG_INFO("Requesting table upload parameters");

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::LeaderOrFollower, CellTag_);
        TObjectServiceProxy proxy(channel);

        auto req =  TTableYPathProxy::GetUploadParams(objectIdPath);
        if (append && sorted) {
            req->set_fetch_last_key(true);
        }
        SetTransactionId(req, UploadTransaction_);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            "Error requesting upload parameters for table %v",
            path);

        const auto& rsp = rspOrError.Value();
        ChunkListId_ = FromProto<TChunkListId>(rsp->chunk_list_id());
        auto lastKey = FromProto<TOwningKey>(rsp->last_key());
        if (lastKey) {
            YCHECK(lastKey.GetCount() >= KeyColumns_.size());
            LastKey_ = TOwningKey(lastKey.Begin(), lastKey.Begin() + KeyColumns_.size());
        }

        LOG_INFO("Table upload parameters received (ChunkListId: %v, HasLastKey: %v)",
            ChunkListId_,
            static_cast<bool>(LastKey_));
    }

    UnderlyingWriter_ = CreateSchemalessMultiChunkWriter(
        Config_,
        Options_,
        NameTable_,
        KeyColumns_,
        LastKey_,
        Client_,
        CellTag_,
        UploadTransaction_->GetId(),
        ChunkListId_,
        true,
        Throttler_,
        BlockCache_);

    WaitFor(UnderlyingWriter_->Open())
        .ThrowOnError();

    LOG_INFO("Table opened");
}

void TSchemalessTableWriter::DoClose()
{
    const auto& path = RichPath_.GetPath();
    auto objectIdPath = FromObjectId(ObjectId_);

    LOG_INFO("Closing table");

    {
        auto error = WaitFor(UnderlyingWriter_->Close());
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error closing chunk writer");
    }

    UploadTransaction_->Ping();
    UploadTransaction_->Detach();

    auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TTableYPathProxy::EndUpload(objectIdPath);
        *req->mutable_statistics() = UnderlyingWriter_->GetDataStatistics();
        ToProto(req->mutable_key_columns(), KeyColumns_);
        SetTransactionId(req, UploadTransaction_);
        GenerateMutationId(req);
        batchReq->AddRequest(req, "end_upload");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        GetCumulativeError(batchRspOrError),
        "Error finishing upload to table %v",
        path);

    LOG_INFO("Table closed");
}

TNameTablePtr TSchemalessTableWriter::GetNameTable() const
{
    return NameTable_;
}

bool TSchemalessTableWriter::IsSorted() const
{
    return UnderlyingWriter_->IsSorted();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessWriterPtr CreateSchemalessTableWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const TRichYPath& richPath,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IClientPtr client,
    ITransactionPtr transaction,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    return New<TSchemalessTableWriter>(
        config,
        options,
        richPath,
        nameTable,
        keyColumns,
        client,
        transaction,
        throttler,
        blockCache);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
