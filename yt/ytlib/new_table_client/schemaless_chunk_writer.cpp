#include "stdafx.h"

#include "schemaless_chunk_writer.h"

#include "chunk_meta_extensions.h"
#include "chunk_writer_base.h"
#include "config.h"
#include "name_table.h"
#include "partitioner.h"
#include "schemaless_block_writer.h"
#include "schemaless_row_reorderer.h"

#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/chunk_writer.h>
#include <ytlib/chunk_client/encoding_chunk_writer.h>
#include <ytlib/chunk_client/multi_chunk_writer_base.h>
#include <ytlib/chunk_client/chunk_writer.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/transaction_client/helpers.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/transaction_client/transaction_manager.h>

// TTableYPathProxy
#include <ytlib/table_client/table_ypath_proxy.h>
// TKeyColumnsExt
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/ypath/rich.h>

#include <core/concurrency/scheduler.h>

#include <core/rpc/channel.h>

#include <core/ytree/attribute_helpers.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NProto;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

using NNodeTrackerClient::TNodeDirectoryPtr;

using NTableClient::TTableYPathProxy;

////////////////////////////////////////////////////////////////////////////////

auto& Logger = TableClientLogger;

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
        IChunkWriterPtr asyncWriter,
        const TKeyColumns& keyColumns = TKeyColumns());

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override;

    virtual TNameTablePtr GetNameTable() const override;

    virtual bool IsSorted() const override;

private:
    TNameTablePtr NameTable_;

    THorizontalSchemalessBlockWriter* CurrentBlockWriter_;

    virtual ETableChunkFormat GetFormatVersion() const override;
    virtual IBlockWriter* CreateBlockWriter() override;

    virtual void PrepareChunkMeta() override;

};

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
TSchemalessChunkWriter<TBase>::TSchemalessChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TNameTablePtr nameTable,
    IChunkWriterPtr asyncWriter,
    const TKeyColumns& keyColumns)
    : TBase(config, options, asyncWriter, keyColumns)
    , NameTable_(nameTable)
{ }

template <class TBase>
bool TSchemalessChunkWriter<TBase>::Write(const std::vector<TUnversionedRow>& rows)
{
    YCHECK(CurrentBlockWriter_);

    for (auto row : rows) {
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

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkWriterPtr CreateSchemalessChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    NChunkClient::IChunkWriterPtr chunkWriter)
{
    if (keyColumns.empty()) {
        return New<TSchemalessChunkWriter<TSequentialChunkWriterBase>>(config, options, nameTable, chunkWriter);
    } else {
        return New<TSchemalessChunkWriter<TSortedChunkWriterBase>>(config, options, nameTable, chunkWriter, keyColumns);
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
        IChunkWriterPtr asyncWriter,
        const TKeyColumns& keyColumns,
        IPartitioner* partitioner);

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override;

    virtual TNameTablePtr GetNameTable() const override;

    virtual i64 GetDataSize() const override;

    virtual TChunkMeta GetSchedulerMeta() const override;

    virtual i64 GetMetaSize() const override;

    virtual bool IsSorted() const override;

private:
    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;

    TPartitionsExt PartitionsExt_;

    IPartitioner* Partitioner_;

    std::vector<std::unique_ptr<THorizontalSchemalessBlockWriter>> BlockWriters_;

    i64 CurrentBufferCapacity_ = 0;

    int LargestPartitionIndex_ = 0;
    i64 LargestPartitionSize_ = 0;

    i64 BlockReserveSize_;


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
    IChunkWriterPtr asyncWriter,
    const TKeyColumns& keyColumns,
    IPartitioner* partitioner)
    : TChunkWriterBase(config, options, asyncWriter)
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
    for (auto& row : rows) {
        WriteRow(row);
    }

    return EncodingChunkWriter_->IsReady();
}

void TPartitionChunkWriter::WriteRow(TUnversionedRow row)
{
    ++RowCount_;
    DataWeight_ += GetDataWeight(row);

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

void TPartitionChunkWriter::FlushBlock(int partitionIndex)
{
    auto& blockWriter = BlockWriters_[partitionIndex];
    auto block = blockWriter->FlushBlock();
    block.Meta.set_partition_index(partitionIndex);

    RegisterBlock(block);
}

void TPartitionChunkWriter::InitLargestPartition()
{
    LargestPartitionIndex_ = 0;
    LargestPartitionSize_ = BlockWriters_.front()->GetCapacity();
    for (int partitionIndex = 1; partitionIndex < BlockWriters_.size(); ++partitionIndex) {
        auto& blockWriter = BlockWriters_[partitionIndex];
        if (blockWriter->GetCapacity() > LargestPartitionSize_) {
            LargestPartitionSize_ = blockWriter->GetCapacity();
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
    NYT::ToProto(keyColumnsExt.mutable_names(), KeyColumns_);
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
    IChunkWriterPtr asyncWriter,
    IPartitioner* partitioner)
{
    return New<TPartitionChunkWriter>(
        config,
        options,
        nameTable,
        asyncWriter,
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
    virtual const std::vector<TChunkSpec>& GetWrittenChunks() const override;
    virtual TNodeDirectoryPtr GetNodeDirectory() const override;
    virtual TDataStatistics GetDataStatistics() const override;

};

////////////////////////////////////////////////////////////////////////////////

TReorderingSchemalessMultiChunkWriter::TReorderingSchemalessMultiChunkWriter(
    const TKeyColumns& keyColumns,
    TNameTablePtr nameTable,
    ISchemalessMultiChunkWriterPtr underlyingWriter)
    : MemoryPool_(TReorderingSchemalessWriterPoolTag())
    , RowReorderer_(nameTable, keyColumns)
    , UnderlyingWriter_(underlyingWriter)
    , KeyColumnCount_(keyColumns.size())
{ 
    if (IsSorted()) {
        std::vector<TUnversionedValue> key(
            KeyColumnCount_,
            MakeUnversionedSentinelValue(EValueType::Min, 0));
        LastKey_ = TOwningKey(key.data(), key.data() + KeyColumnCount_);
    }
}

bool TReorderingSchemalessMultiChunkWriter::CheckSortOrder(TUnversionedRow lhs, TUnversionedRow rhs)
{
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
        "Sort order violation: %v >= %v", 
        leftBuilder.FinishRow().Get(), 
        rightBuilder.FinishRow().Get());
    return false;
}

bool TReorderingSchemalessMultiChunkWriter::Write(const std::vector<TUnversionedRow>& rows)
{
    std::vector<TUnversionedRow> reorderedRows;
    reorderedRows.reserve(rows.size());

    for (const auto& row : rows) {
        LOG_DEBUG("BEFORE %v", row);
        reorderedRows.push_back(RowReorderer_.ReorderRow(row, &MemoryPool_));
        LOG_DEBUG("AFTER %v", reorderedRows.back());
    }

    if (IsSorted() && !reorderedRows.empty()) {
        if (!CheckSortOrder(LastKey_.Get(), reorderedRows.front())) {
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

const std::vector<TChunkSpec>& TReorderingSchemalessMultiChunkWriter::GetWrittenChunks() const
{
    return UnderlyingWriter_->GetWrittenChunks();
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
        IChannelPtr masterChannel,
        const TTransactionId& transactionId,
        const TChunkListId& parentChunkListId,
        std::function<ISchemalessChunkWriterPtr(IChunkWriterPtr)> createChunkWriter,
        TNameTablePtr nameTable,
        bool isSorted)
        : TBase(config, options, masterChannel, transactionId, parentChunkListId, createChunkWriter)
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
    TNameTablePtr NameTable_;
    bool IsSorted_;

};

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreateSchemalessMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IChannelPtr masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    bool reorderValues)
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
            underlyingWriter);
    };

    bool isSorted = !keyColumns.empty();
    auto writer = New<TWriter>(
        config,
        options,
        masterChannel,
        transactionId,
        parentChunkListId,
        createChunkWriter,
        nameTable,
        isSorted);

    if (reorderValues && isSorted)
        return New<TReorderingSchemalessMultiChunkWriter>(keyColumns, nameTable, writer);
    else
        return writer;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreatePartitionMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IChannelPtr masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    std::unique_ptr<IPartitioner> partitioner)
{
    YCHECK(!keyColumns.empty());

    typedef TMultiChunkWriterBase<
        ISchemalessMultiChunkWriter,
        ISchemalessChunkWriter,
        const std::vector<TUnversionedRow>&> TPartitionMultiChunkWriterBase;

    typedef TSchemalessMultiChunkWriter<TPartitionMultiChunkWriterBase> TWriter;

    // Convert to shared_ptr to capture partitioner in lambda.
    std::shared_ptr<IPartitioner> sharedPartitioner(std::move(partitioner));
    auto createChunkWriter = [=] (IChunkWriterPtr underlyingWriter) {
        return CreatePartitionChunkWriter(
            config,
            options,
            nameTable,
            keyColumns,
            underlyingWriter,
            sharedPartitioner.get());
    };

    auto writer = New<TWriter>(
        config,
        options,
        masterChannel,
        transactionId,
        parentChunkListId,
        createChunkWriter,
        nameTable,
        false);

    return New<TReorderingSchemalessMultiChunkWriter>(keyColumns, nameTable, writer);
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessTableWriter
    : public ISchemalessWriter
    , public TTransactionListener
{
public:
    TSchemalessTableWriter(
        TTableWriterConfigPtr config,
        const TRichYPath& richPath,
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns,
        IChannelPtr masterChannel,
        TTransactionPtr transaction,
        TTransactionManagerPtr transactionManager);

    virtual TFuture<void> Open() override;
    virtual bool Write(const std::vector<TUnversionedRow>& rows) override;
    virtual TFuture<void> GetReadyEvent() override;
    virtual TFuture<void> Close() override;
    virtual TNameTablePtr GetNameTable() const override;
    virtual bool IsSorted() const override;

private:
    NLog::TLogger Logger;

    TTableWriterConfigPtr Config_;
    TTableWriterOptionsPtr Options_;

    TRichYPath RichPath_;
    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;
    IChannelPtr MasterChannel_;
    TTransactionPtr Transaction_;
    TTransactionManagerPtr TransactionManager_;

    TTransactionId TransactionId_;

    TTransactionPtr UploadTransaction_;
    TChunkListId ChunkListId_;

    ISchemalessWriterPtr UnderlyingWriter_;


    void DoOpen();
    void FetchTableInfo();
    void CreateUploadTransaction();

    void DoClose();

};

////////////////////////////////////////////////////////////////////////////////

TSchemalessTableWriter::TSchemalessTableWriter(
    TTableWriterConfigPtr config,
    const TRichYPath& richPath,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IChannelPtr masterChannel,
    TTransactionPtr transaction,
    TTransactionManagerPtr transactionManager)
    : Logger(TableClientLogger)
    , Config_(config)
    , Options_(New<TTableWriterOptions>())
    , RichPath_(richPath)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , MasterChannel_(masterChannel)
    , Transaction_(transaction)
    , TransactionManager_(transactionManager)
    , TransactionId_(transaction ? transaction->GetId() : NullTransactionId)
{ 
    YCHECK(masterChannel);

    Logger.AddTag("Path: v, TransactihonId: %v",
        RichPath_.GetPath(),
        TransactionId_);
}

TFuture<void> TSchemalessTableWriter::Open()
{
    LOG_INFO("Opening table writer");

    return BIND(&TSchemalessTableWriter::DoOpen, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
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
        return MakeFuture(TError("Transaction aborted (Path: %v, TransactionId: %v)", 
            RichPath_.GetPath(),
            TransactionId_));
    }

    return UnderlyingWriter_->GetReadyEvent();
}

TFuture<void> TSchemalessTableWriter::Close()
{
    return BIND(&TSchemalessTableWriter::DoClose, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

void TSchemalessTableWriter::CreateUploadTransaction()
{
    TTransactionStartOptions options;
    options.ParentId = TransactionId_;
    options.EnableUncommittedAccounting = false;

    auto attributes = CreateEphemeralAttributes();
    attributes->Set("title", Format("Table upload to %v", RichPath_.GetPath()));
    options.Attributes = std::move(attributes);

    auto transactionOrError = WaitFor(TransactionManager_->Start(
        ETransactionType::Master,
        options));

    THROW_ERROR_EXCEPTION_IF_FAILED(
        transactionOrError, 
        "Error creating upload transaction (Path: %v, TransactionId: %v)", 
        RichPath_.GetPath(),
        TransactionId_);

    UploadTransaction_ = transactionOrError.Value();
    ListenTransaction(UploadTransaction_);
}

void TSchemalessTableWriter::FetchTableInfo()
{
    LOG_INFO("Requesting table info");

    auto path = RichPath_.GetPath();
    bool clear = !KeyColumns_.empty() || !RichPath_.GetAppend();

    TObjectServiceProxy objectProxy(MasterChannel_);
    auto batchReq = objectProxy.ExecuteBatch();

    {
        auto req = TCypressYPathProxy::Get(path);
        SetTransactionId(req, UploadTransaction_);
        TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
        attributeFilter.Keys.push_back("type");
        attributeFilter.Keys.push_back("replication_factor");
        attributeFilter.Keys.push_back("compression_codec");
        attributeFilter.Keys.push_back("erasure_codec");
        attributeFilter.Keys.push_back("account");
        attributeFilter.Keys.push_back("vital");

        if (!KeyColumns_.empty()) {
            attributeFilter.Keys.push_back("row_count");
        }

        ToProto(req->mutable_attribute_filter(), attributeFilter);
        batchReq->AddRequest(req, "get_attributes");
    } {
        auto req = TTableYPathProxy::PrepareForUpdate(path);
        SetTransactionId(req, UploadTransaction_);
        GenerateMutationId(req);
        req->set_mode(clear 
            ? static_cast<int>(EUpdateMode::Overwrite) 
            : static_cast<int>(EUpdateMode::Append));
        batchReq->AddRequest(req, "prepare_for_update");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        GetCumulativeError(batchRspOrError), 
        "Error requesting table info (Path: %v, TransactionId: %v)", 
        path,
        TransactionId_);
    const auto& batchRsp = batchRspOrError.Value();

    {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
        auto node = ConvertToNode(TYsonString(rspOrError.Value()->value()));
        const auto& attributes = node->Attributes();

        auto type = attributes.Get<EObjectType>("type");
        if (type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION(
                "Invalid type of cypress node: expected %Qlv, actual %Qlv (Path: %v, TransactionId: %v)",
                EObjectType::Table,
                type,
                path,
                TransactionId_);
        }

        // TODO(psushin): Keep in sync with OnInputsReceived (operation_controller_detail.cpp).
        if (!KeyColumns_.empty() && RichPath_.GetAppend()) {
            if (attributes.Get<i64>("row_count") > 0) {
                THROW_ERROR_EXCEPTION("Cannot write sorted data into a non-empty table (Path: %v, TransactionId: %v)",
                    path,
                    TransactionId_);
            }
        }

        Options_->ReplicationFactor = attributes.Get<int>("replication_factor");
        Options_->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
        Options_->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec");
        Options_->Account = attributes.Get<Stroka>("account");
        Options_->ChunksVital = attributes.Get<bool>("vital");
    } {
        using NYT::FromProto;

        auto rspOrError = batchRsp->GetResponse<TTableYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
        ChunkListId_ = FromProto<TChunkListId>(rspOrError.Value()->chunk_list_id());
    }

    LOG_INFO("Table info received (ChunkListId: %v)", ChunkListId_);
}

void TSchemalessTableWriter::DoOpen()
{
    CreateUploadTransaction();
    LOG_INFO("Upload transaction created (TransactionId: %v)", UploadTransaction_->GetId());

    FetchTableInfo();

    UnderlyingWriter_ = CreateSchemalessMultiChunkWriter(
        Config_,
        Options_,
        NameTable_,
        KeyColumns_,
        MasterChannel_,
        UploadTransaction_->GetId(),
        ChunkListId_,
        true);

    auto error = WaitFor(UnderlyingWriter_->Open());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        error, 
        "Error opening table chunk writer (Path: %v, TransactionId: %v)", 
        RichPath_.GetPath(),
        TransactionId_);

    if (Transaction_) {
        ListenTransaction(Transaction_);
    }
}

void TSchemalessTableWriter::DoClose()
{
    LOG_INFO("Closing table writer");
    {
        auto error = WaitFor(UnderlyingWriter_->Close());
        THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error closing chunk writer");
        LOG_INFO("Chunk writer closed");
    }

    if (!KeyColumns_.empty()) {
        using NYT::ToProto;

        auto path = RichPath_.GetPath();
        LOG_INFO("Marking table as sorted by %v",
            ConvertToYsonString(KeyColumns_, NYson::EYsonFormat::Text).Data());

        auto req = TTableYPathProxy::SetSorted(path);
        SetTransactionId(req, UploadTransaction_);
        GenerateMutationId(req);
        ToProto(req->mutable_key_columns(), KeyColumns_);

        TObjectServiceProxy objectProxy(MasterChannel_);
        auto rspOrError = WaitFor(objectProxy.Execute(req));

        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError, 
            "Error marking table as sorted (Path: %v, TransactionId: %v)", 
            RichPath_.GetPath(),
            TransactionId_)
    }

    LOG_INFO("Committing upload transaction");
    {
        auto error = WaitFor(UploadTransaction_->Commit());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            error, 
            "Error committing upload transaction (Path: %v, TransactionId: %v)", 
            RichPath_.GetPath(),
            TransactionId_);
        LOG_INFO("Upload transaction committed");
        LOG_INFO("Table writer closed");
    }
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
    const TRichYPath& richPath,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IChannelPtr masterChannel,
    TTransactionPtr transaction,
    TTransactionManagerPtr transactionManager)
{
    return New<TSchemalessTableWriter>(config, richPath, nameTable, keyColumns, masterChannel, transaction, transactionManager);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
