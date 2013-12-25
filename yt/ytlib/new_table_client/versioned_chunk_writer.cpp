#include "stdafx.h"

#include "chunk_meta_extensions.h"
#include "config.h"
#include "versioned_block_writer.h"
#include "versioned_chunk_writer.h"
#include "versioned_writer.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_client/encoding_chunk_writer.h>
#include <ytlib/chunk_client/dispatcher.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

template<class TBlockWriter>
class TVersionedChunkWriter
    : public IVersionedWriter
{
public:
    TVersionedChunkWriter(
        const TChunkWriterConfigPtr& config,
        const TChunkWriterOptionsPtr& options,
        const TTableSchema& schema,
        const TKeyColumns& keyColumns,
        const IAsyncWriterPtr& asyncWriter);

    virtual TAsyncError Open() override;

    virtual bool Write(const std::vector<TVersionedRow>& rows) override;

    virtual TAsyncError Close() override;

    virtual TAsyncError GetReadyEvent() override;
private:
    TChunkWriterConfigPtr Config_;
    TTableSchema Schema_;
    TKeyColumns KeyColumns_;

    TEncodingChunkWriterPtr EncodingChunkWriter_;

    TOwningKey LastKey;
    std::unique_ptr<TBlockWriter> BlockWriter_;

    TBlockMetaExt BlockMetaExt_;
    TBlockIndexExt BlockIndexExt_;
    TBoundaryKeysExt BoundaryKeysExt_;

    i64 RowCount_;

    void WriteRow(
        const TVersionedRow& row,
        const TUnversionedValue* beginPreviousKey,
        const TUnversionedValue* endPreviousKey);

    void FinishBlockIfLarge(const TVersionedRow& row);
    void FinishBlock();

    TError DoClose();

};

////////////////////////////////////////////////////////////////////////////////

template<class TBlockWriter>
TVersionedChunkWriter<TBlockWriter>::TVersionedChunkWriter(
    const TChunkWriterConfigPtr& config,
    const TChunkWriterOptionsPtr& options,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    const IAsyncWriterPtr& asyncWriter)
    : Config_(config)
    , Schema_(schema)
    , KeyColumns_(keyColumns)
    , EncodingChunkWriter_(New<TEncodingChunkWriter>(config, options, asyncWriter))
    , LastKey(static_cast<TUnversionedValue*>(nullptr), static_cast<TUnversionedValue*>(nullptr))
    , BlockWriter_(new TBlockWriter(Schema_, KeyColumns_))
{
    YCHECK(Schema_.Columns().size() > 0);
    YCHECK(KeyColumns_.size() > 0);
    YCHECK(ValidateKeyColumns(Schema_, KeyColumns_).IsOK());
}

template<class TBlockWriter>
TAsyncError TVersionedChunkWriter<TBlockWriter>::Open()
{
    return MakeFuture(TError());
}

template<class TBlockWriter>
bool TVersionedChunkWriter<TBlockWriter>::Write(const std::vector<TVersionedRow>& rows)
{
    YCHECK(rows.size() > 0);

    if (RowCount_ == 0) {
        ToProto(
            BoundaryKeysExt_.mutable_first(),
            TOwningKey(rows.front().BeginKeys(), rows.front().EndKeys()));
    }

    WriteRow(rows.front(), LastKey.Begin(), LastKey.End());
    FinishBlockIfLarge(rows.front());

    for (int i = 1; i < rows.size(); ++i) {
        WriteRow(rows[i], rows[i - 1].BeginKeys(), rows[i - 1].EndKeys());
        FinishBlockIfLarge(rows.front());
    }

    LastKey = TOwningKey(rows.back().BeginKeys(), rows.back().EndKeys());
    return EncodingChunkWriter_->IsReady();
}

template<class TBlockWriter>
TAsyncError TVersionedChunkWriter<TBlockWriter>::GetReadyEvent()
{
    return EncodingChunkWriter_->GetReadyEvent();
}

template<class TBlockWriter>
TAsyncError TVersionedChunkWriter<TBlockWriter>::Close()
{
    if (RowCount_ == 0) {
        // Empty chunk.
        return MakeFuture(TError());
    }

    return BIND(&TVersionedChunkWriter<TBlockWriter>::DoClose, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
        .Run();
}

template<class TBlockWriter>
void TVersionedChunkWriter<TBlockWriter>::WriteRow(
    const TVersionedRow& row,
    const TUnversionedValue* beginPreviousKey,
    const TUnversionedValue* endPreviousKey)
{
    ++RowCount_;
    BlockWriter_->WriteRow(row, beginPreviousKey, endPreviousKey);
}

template<class TBlockWriter>
void TVersionedChunkWriter<TBlockWriter>::FinishBlockIfLarge(const TVersionedRow& row)
{
    if (BlockWriter_->GetBlockSize() < Config_->BlockSize) {
        return;
    }

    // Emit block index
    ToProto(BlockIndexExt_.add_entries(), row.BeginKeys(), row.EndKeys());

    FinishBlock();
    BlockWriter_.reset(new TBlockWriter(Schema_, KeyColumns_));
}

template<class TBlockWriter>
void TVersionedChunkWriter<TBlockWriter>::FinishBlock()
{
    auto block = BlockWriter_->FlushBlock();
    block.Meta.set_chunk_row_count(RowCount_);

    BlockMetaExt_.add_entries()->Swap(&block.Meta);
    EncodingChunkWriter_->WriteBlock(std::move(block.Data));
}

template<class TBlockWriter>
TError TVersionedChunkWriter<TBlockWriter>::DoClose()
{
    using NYT::ToProto;

    if (BlockWriter_->GetRowCount() > 0) {
        FinishBlock();
    }

    ToProto(BoundaryKeysExt_.mutable_last(), LastKey);

    auto& meta = EncodingChunkWriter_->Meta();
    meta.set_type(EChunkType::Table);
    meta.set_version(TBlockWriter::FormatVersion);

    SetProtoExtension(meta.mutable_extensions(), ToProto<TTableSchemaExt>(Schema_));

    TKeyColumnsExt keyColumnsExt;
    for (auto name : KeyColumns_) {
        keyColumnsExt.add_names(name);
    }

    SetProtoExtension(meta.mutable_extensions(), keyColumnsExt);

    SetProtoExtension(meta.mutable_extensions(), BlockMetaExt_);
    SetProtoExtension(meta.mutable_extensions(), BlockIndexExt_);

    auto& miscExt = EncodingChunkWriter_->MiscExt();
    miscExt.set_sorted(true);
    miscExt.set_row_count(RowCount_);

    return EncodingChunkWriter_->Close();
}

////////////////////////////////////////////////////////////////////////////////

IVersionedWriterPtr CreateVersionedChunkWriter(
    const TChunkWriterConfigPtr& config,
    const TChunkWriterOptionsPtr& options,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    const IAsyncWriterPtr& asyncWriter)
{
    return New< TVersionedChunkWriter<TSimpleVersionedBlockWriter> >(
        config,
        options,
        schema, 
        keyColumns,
        asyncWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
