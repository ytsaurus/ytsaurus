#include "stdafx.h"

#include "schemaless_chunk_writer.h"

#include "chunk_meta_extensions.h"
#include "chunk_writer_base.h"
#include "config.h"
#include "name_table.h"
#include "partitioner.h"
#include "schemaless_block_writer.h"

#include <ytlib/chunk_client/chunk_writer.h>
#include <ytlib/chunk_client/encoding_chunk_writer.h>
#include <ytlib/chunk_client/multi_chunk_writer_base.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/hydra/rpc_helpers.h>

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

using NHydra::GenerateMutationId;

using NTableClient::TTableYPathProxy;

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
        IAsyncWriterPtr asyncWriter,
        const TKeyColumns& keyColumns,
        IPartitioner* partitioner);

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override;

    virtual i64 GetDataSize() const override;

    virtual TChunkMeta GetSchedulerMeta() const override;

    virtual i64 GetMetaSize() const override;

private:
    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;

    TPartitionsExt PartitionsExt_;

    IPartitioner* Partitioner_;

    std::vector<std::unique_ptr<THorizontalSchemalessBlockWriter>> BlockWriters_;

    i64 CurrentBufferSize_;

    int LargestPartitionIndex_;
    i64 LargestPartitionSize_;


    void WriteRow(TUnversionedRow row);

    void InitLargestPartition();
    void FlushBlock(int partitionIndex);

    virtual TError DoClose() override;
    virtual void PrepareChunkMeta() override;

    virtual ETableChunkFormat GetFormatVersion() const override;

};

////////////////////////////////////////////////////////////////////////////////

TPartitionChunkWriter::TPartitionChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TNameTablePtr nameTable,
    IAsyncWriterPtr asyncWriter,
    const TKeyColumns& keyColumns,
    IPartitioner* partitioner)
    : TChunkWriterBase(config, options, asyncWriter)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , Partitioner_(partitioner)
    , CurrentBufferSize_(0)
    , LargestPartitionIndex_(0)
    , LargestPartitionSize_(0)
{
    int partitionCount = Partitioner_->GetPartitionCount();
    BlockWriters_.reserve(partitionCount);

    for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
        BlockWriters_.emplace_back(new THorizontalSchemalessBlockWriter());

        auto* partitionAttributes = PartitionsExt_.add_partitions();
        partitionAttributes->set_row_count(0);
        partitionAttributes->set_uncompressed_data_size(0);
    }
}

bool TPartitionChunkWriter::Write(const std::vector<TUnversionedRow> &rows)
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

    i64 oldSize = blockWriter->GetBlockSize();
    blockWriter->WriteRow(row);

    i64 newSize = blockWriter->GetBlockSize();

    i64 sizeDelta = newSize - oldSize;
    CurrentBufferSize_ += sizeDelta;

    auto* partitionAttributes = PartitionsExt_.mutable_partitions(partitionIndex);
    partitionAttributes->set_row_count(partitionAttributes->row_count() + 1);
    partitionAttributes->set_uncompressed_data_size(partitionAttributes->uncompressed_data_size() + sizeDelta);

    if (newSize > LargestPartitionSize_) {
        LargestPartitionIndex_ = partitionIndex;
        LargestPartitionSize_ = newSize;
    }

    if (LargestPartitionSize_ >= Config_->BlockSize || CurrentBufferSize_ >= Config_->MaxBufferSize) {
        FlushBlock(LargestPartitionIndex_);
        BlockWriters_[LargestPartitionIndex_].reset(new THorizontalSchemalessBlockWriter());

        CurrentBufferSize_ -= LargestPartitionSize_;
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
    return TChunkWriterBase::GetDataSize() + CurrentBufferSize_;
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

TError TPartitionChunkWriter::DoClose()
{
    for (int partitionIndex = 0; partitionIndex < BlockWriters_.size(); ++partitionIndex) {
        if (BlockWriters_[partitionIndex]->GetRowCount() > 0) {
            FlushBlock(partitionIndex);
        }
    }

    return TChunkWriterBase::DoClose();
}

void TPartitionChunkWriter::PrepareChunkMeta()
{
    TChunkWriterBase::PrepareChunkMeta();

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

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkWriterPtr CreatePartitionChunkWriter(
    TChunkWriterConfigPtr config,
    TChunkWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IAsyncWriterPtr asyncWriter,
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

class TSchemalessMultiChunkWriter
    : public TMultiChunkWriterBase<ISchemalessChunkWriter>
    , public ISchemalessMultiChunkWriter
{
public:
    TSchemalessMultiChunkWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns,
        IChannelPtr masterChannel,
        const TTransactionId& transactionId,
        const TChunkListId& parentChunkListId)
        : TMultiChunkWriterBase<ISchemalessChunkWriter>(
              config,
              options,
              masterChannel,
              transactionId,
              parentChunkListId)
        , Config_(config)
        , Options_(options)
        , NameTable_(nameTable)
        , KeyColumns_(keyColumns)
    { }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        if (!VerifyActive()) {
            return false;
        }

        // Return true if current writer is ready for more data and
        // we didn't switch to the next chunk.
        bool readyForMore = CurrentWriter_->Write(rows);
        bool switched = false;
        if (readyForMore) {
            switched = TrySwitchSession();
        }
        return readyForMore && !switched;
    }


protected:
    TTableWriterConfigPtr Config_;
    TTableWriterOptionsPtr Options_;
    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;

private:
    virtual ISchemalessChunkWriterPtr CreateChunkWriter(IChunkWriterPtr underlyingWriter) override
    {
        return CreateSchemalessChunkWriter(Config_, Options_, NameTable_, KeyColumns_, underlyingWriter);
    }

};

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

private:
    TKeyColumns KeyColumns_;
    TNameTablePtr NameTable_;
    ISchemalessMultiChunkWriterPtr UnderlyingWriter_;

    std::vector<int> IdMapping_;
    TChunkedMemoryPool MemoryPool_;
    std::vector<TUnversionedValue> EmptyKey_;


    TUnversionedRow ReorderRow(const TUnversionedRow& row);

    virtual TAsyncError Open() override;
    virtual TAsyncError GetReadyEvent() override;
    virtual TAsyncError Close() override;

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
    : KeyColumns_(keyColumns)
    , NameTable_(nameTable)
    , UnderlyingWriter_(underlyingWriter)
    , MemoryPool_(TReorderingSchemalessWriterPoolTag())
{ 
    for (int i = 0; i < KeyColumns_.size(); ++i) {
        auto id = NameTable_->GetIdOrRegisterName(KeyColumns_[i]);
        if (id >= IdMapping_.size()) {
            IdMapping_.resize(id + 1, -1);
        } 
        IdMapping_[id] = i;
    }

    EmptyKey_.resize(KeyColumns_.size(), MakeUnversionedSentinelValue(EValueType::Null));
}

bool TReorderingSchemalessMultiChunkWriter::Write(const std::vector<TUnversionedRow>& rows)
{
    std::vector<TUnversionedRow> reorderedRows;
    reorderedRows.reserve(rows.size());

    for (const auto& row : rows) {
        reorderedRows.push_back(ReorderRow(row));
    }

    auto result = UnderlyingWriter_->Write(reorderedRows);
    MemoryPool_.Clear();

    return result;
}

TUnversionedRow TReorderingSchemalessMultiChunkWriter::ReorderRow(const TUnversionedRow& row)
{
    int valueCount = KeyColumns_.size() + row.GetCount();
    TUnversionedRow result = TUnversionedRow::Allocate(&MemoryPool_, valueCount);

    // Initialize with empty key.
    ::memcpy(result.Begin(), EmptyKey_.data(), KeyColumns_.size() * sizeof(TUnversionedValue));

    int nextValueIndex = KeyColumns_.size();
    for (auto it = row.Begin(); it != row.End(); ++it) {
        const auto& value = *it;
        if (value.Id < IdMapping_.size()) {
            int keyIndex = IdMapping_[value.Id];
            if (keyIndex >= 0) {
                result.Begin()[keyIndex] = value;
                --valueCount;
                continue;
            }
        }
        result.Begin()[nextValueIndex] = value;
        ++nextValueIndex;
    }

    result.SetCount(valueCount);
    return result;
}

TAsyncError TReorderingSchemalessMultiChunkWriter::Open()
{
    return UnderlyingWriter_->Open();
}

TAsyncError TReorderingSchemalessMultiChunkWriter::GetReadyEvent()
{
    return UnderlyingWriter_->GetReadyEvent();
}

TAsyncError TReorderingSchemalessMultiChunkWriter::Close()
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
    auto writer = New<TSchemalessMultiChunkWriter>(
        config,
        options,
        nameTable,
        keyColumns,
        masterChannel,
        transactionId,
        parentChunkListId);

    if (reorderValues && !keyColumns.empty())
        return New<TReorderingSchemalessMultiChunkWriter>(keyColumns, nameTable, writer);
    else
        return writer;
}

////////////////////////////////////////////////////////////////////////////////

class TPartitionMultiChunkWriter
    : public TSchemalessMultiChunkWriter
{
public:
    TPartitionMultiChunkWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        TNameTablePtr nameTable,
        const TKeyColumns& keyColumns,
        IChannelPtr masterChannel,
        const TTransactionId& transactionId,
        const TChunkListId& parentChunkListId,
        std::unique_ptr<IPartitioner> partitioner)
        : TSchemalessMultiChunkWriter(
              config,
              options,
              nameTable,
              keyColumns,
              masterChannel,
              transactionId,
              parentChunkListId)
        , Partitioner_(std::move(partitioner))
    { }

private:
    std::unique_ptr<IPartitioner> Partitioner_;

    virtual ISchemalessChunkWriterPtr CreateChunkWriter(IAsyncWriterPtr underlyingWriter) override
    {
        return CreatePartitionChunkWriter(
            Config_,
            Options_,
            NameTable_,
            KeyColumns_,
            underlyingWriter,
            Partitioner_.get());
    }

};

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
    auto writer = New<TPartitionMultiChunkWriter>(
        config,
        options,
        nameTable,
        keyColumns,
        masterChannel,
        transactionId,
        parentChunkListId,
        std::move(partitioner));

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

    virtual TAsyncError Open() override;
    virtual bool Write(const std::vector<TUnversionedRow>& rows) override;
    virtual TAsyncError GetReadyEvent() override;
    virtual TAsyncError Close() override;

private:
    NLog::TTaggedLogger Logger;

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


    TError DoOpen();
    TError FetchTableInfo();
    TError CreateUploadTransaction();

    TError DoClose();

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
    : Logger(TableWriterLogger)
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

    Logger.AddTag(Sprintf("Path: %s, TransactihonId: %s",
        ~RichPath_.GetPath(),
        ~ToString(TransactionId_)));
}

TAsyncError TSchemalessTableWriter::Open()
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

TAsyncError TSchemalessTableWriter::GetReadyEvent()
{
    if (IsAborted()) {
        return MakeFuture(TError("Transaction aborted (Path: %s, TransactionId: %s)", 
            ~RichPath_.GetPath(),
            ~ToString(TransactionId_)));
    }

    return UnderlyingWriter_->GetReadyEvent();
}

TAsyncError TSchemalessTableWriter::Close()
{
    return BIND(&TSchemalessTableWriter::DoClose, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

TError TSchemalessTableWriter::CreateUploadTransaction()
{
    TTransactionStartOptions options;
    options.ParentId = TransactionId_;
    options.EnableUncommittedAccounting = false;

    auto attributes = CreateEphemeralAttributes();
    attributes->Set("title", Sprintf("Table upload to %s", ~RichPath_.GetPath()));
    options.Attributes = attributes.get();

    auto transactionOrError = WaitFor(TransactionManager_->Start(
        ETransactionType::Master,
        options));

    if (!transactionOrError.IsOK()) {
        return TError("Error creating upload transaction (Path: %s, TransactionId: %s)", 
            ~RichPath_.GetPath(),
            ~ToString(TransactionId_)) << transactionOrError;
    }

    UploadTransaction_ = transactionOrError.Value();
    ListenTransaction(UploadTransaction_);

    return TError();
}

TError TSchemalessTableWriter::FetchTableInfo()
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
        req->set_mode(clear ? EUpdateMode::Overwrite : EUpdateMode::Append);
        batchReq->AddRequest(req, "prepare_for_update");
    }

    auto batchRsp = WaitFor(batchReq->Invoke());

    if (!batchRsp->IsOK()) {
        return TError("Error requesting table info (Path: %s, TransactionId: %s)", 
            ~path,
            ~ToString(TransactionId_)) << *batchRsp;
    }

    {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
        if (!rsp->IsOK()) {
            return TError("Error getting table attributes (Path: %s, TransactionId: %s)", 
                ~path,
                ~ToString(TransactionId_)) << *rsp;
        }

        auto node = ConvertToNode(TYsonString(rsp->value()));
        const auto& attributes = node->Attributes();

        auto type = attributes.Get<EObjectType>("type");
        if (type != EObjectType::Table) {
            return TError("Invalid type of cypress node: expected %s, actual %s (Path: %s, TransactionId: %s)",
                ~FormatEnum(EObjectType(EObjectType::Table)).Quote(),
                ~FormatEnum(type).Quote(),
                ~path,
                ~ToString(TransactionId_));
        }

        // TODO(psushin): Keep in sync with OnInputsReceived (operation_controller_detail.cpp).
        if (!KeyColumns_.empty() && RichPath_.GetAppend()) {
            if (attributes.Get<i64>("row_count") > 0) {
                return TError("Cannot write sorted data into a non-empty table (Path: %s, TransactionId: %s)",
                    ~path,
                    ~ToString(TransactionId_));
            }
        }

        Options_->ReplicationFactor = attributes.Get<int>("replication_factor");
        Options_->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
        Options_->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec");
        Options_->Account = attributes.Get<Stroka>("account");
        Options_->ChunksVital = attributes.Get<bool>("vital");
    } {
        using NYT::FromProto;

        auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
        if (!rsp->IsOK()) {
            return TError("Error preparing table for update (Path: %s, TransactionId: %s)", 
                ~path,
                ~ToString(TransactionId_)) << *rsp;
        }
        ChunkListId_ = FromProto<TChunkListId>(rsp->chunk_list_id());
    }

    LOG_INFO("Table info received (ChunkListId: %s)", ~ToString(ChunkListId_));
    return TError();
}

TError TSchemalessTableWriter::DoOpen()
{
    {
        auto error = CreateUploadTransaction();
        RETURN_IF_ERROR(error);
    }

    LOG_INFO("Upload transaction created (TransactionId: %s)", ~ToString(UploadTransaction_->GetId()));

    {
        auto error = FetchTableInfo();
        RETURN_IF_ERROR(error);
    }

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
    if (!error.IsOK()) {
        return TError(
            "Error opening table chunk writer (Path: %s, TransactionId: %s)", 
            ~RichPath_.GetPath(),
            ~ToString(TransactionId_)) << error;
    }

    if (Transaction_) {
        ListenTransaction(Transaction_);
    }

    return TError();
}

TError TSchemalessTableWriter::DoClose()
{
    LOG_INFO("Closing table writer");
    {
        auto error = WaitFor(UnderlyingWriter_->Close());
        if (!error.IsOK()) {
            return TError("Error closing chunk writer") << error;
        }
        LOG_INFO("Chunk writer closed");
    }

    if (!KeyColumns_.empty()) {
        using NYT::ToProto;

        auto path = RichPath_.GetPath();
        LOG_INFO("Marking table as sorted by %s",
            ~ConvertToYsonString(KeyColumns_, NYson::EYsonFormat::Text).Data());

        auto req = TTableYPathProxy::SetSorted(path);
        SetTransactionId(req, UploadTransaction_);
        GenerateMutationId(req);
        ToProto(req->mutable_key_columns(), KeyColumns_);

        TObjectServiceProxy objectProxy(MasterChannel_);
        auto rsp = WaitFor(objectProxy.Execute(req));

        if (!rsp->IsOK()) {
            return TError("Error marking table as sorted (Path: %s, TransactionId: %s)", 
                ~RichPath_.GetPath(),
                ~ToString(TransactionId_)) << *rsp;
        }
    }

    LOG_INFO("Committing upload transaction");
    {
        auto error = WaitFor(UploadTransaction_->Commit());
        if (!error.IsOK()) {
            return TError("Error committing upload transaction (Path: %s, TransactionId: %s)", 
                ~RichPath_.GetPath(),
                ~ToString(TransactionId_)) << error;
        }
        LOG_INFO("Upload transaction committed");
        LOG_INFO("Table writer closed");
    }

    return TError();
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
