#include "stdafx.h"

#include "schemaless_chunk_writer.h"

#include "chunk_meta_extensions.h"
#include "chunk_writer_base.h"
#include "config.h"
#include "name_table.h"
#include "schemaless_block_writer.h"

#include <ytlib/chunk_client/writer.h>
#include <ytlib/chunk_client/encoding_chunk_writer.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer_base.h>

#include <ytlib/transaction_client/public.h>

#include <core/rpc/channel.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NProto;
using namespace NRpc;
using namespace NTransactionClient;

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

    virtual void OnClose() override;

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
void TSchemalessChunkWriter<TBase>::OnClose()
{
    auto& meta = TBase::EncodingChunkWriter_->Meta();
    TNameTableExt nameTableExt;
    ToProto(&nameTableExt, NameTable_);
    SetProtoExtension(meta.mutable_extensions(), nameTableExt);

    TBase::OnClose();
}

template <class TBase>
IBlockWriter* TSchemalessChunkWriter<TBase>::CreateBlockWriter()
{
    CurrentBlockWriter_ = TBase::KeyColumns_.empty()
        ? new THorizontalSchemalessBlockWriter()
        : new THorizontalSchemalessBlockWriter(TBase::KeyColumns_.size());

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
        return New<TSchemalessChunkWriter<TChunkWriterBase>>(config, options, nameTable, chunkWriter);
    } else {
        return New<TSchemalessChunkWriter<TSortedChunkWriterBase>>(config, options, nameTable, chunkWriter, keyColumns);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMultiChunkWriter
    : public TMultiChunkSequentialWriterBase
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
        const TChunkListId& parentChunkListId = NChunkClient::NullChunkListId);

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override;

private:
    TTableWriterConfigPtr Config_;
    TTableWriterOptionsPtr Options_;
    TNameTablePtr NameTable_;
    TKeyColumns KeyColumns_;

    ISchemalessWriter* CurrentWriter_;


    virtual IChunkWriterBasePtr CreateFrontalWriter(IChunkWriterPtr underlyingWriter) override;

};

////////////////////////////////////////////////////////////////////////////////

TSchemalessMultiChunkWriter::TSchemalessMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    IChannelPtr masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId)
    : TMultiChunkSequentialWriterBase(config, options, masterChannel, transactionId, parentChunkListId)
    , Config_(config)
    , Options_(options)
    , NameTable_(nameTable)
    , KeyColumns_(keyColumns)
    , CurrentWriter_(nullptr)
{ }

bool TSchemalessMultiChunkWriter::Write(const std::vector<TUnversionedRow> &rows)
{
    if (!VerifyActive()) {
        return false;
    }

    // Return true if current writer is ready for more data and
    // we didn't switch to the next chunk.
    return CurrentWriter_->Write(rows) && !TrySwitchSession();
}

IChunkWriterBasePtr TSchemalessMultiChunkWriter::CreateFrontalWriter(IChunkWriterPtr underlyingWriter)
{
    auto writer = CreateSchemalessChunkWriter(Config_, Options_, NameTable_, KeyColumns_, underlyingWriter);
    CurrentWriter_ = writer.Get();
    return writer;
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr CreateSchemalessMultiChunkWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns,
    NRpc::IChannelPtr masterChannel,
    const NTransactionClient::TTransactionId& transactionId,
    const NChunkClient::TChunkListId& parentChunkListId)
{
    return New<TSchemalessMultiChunkWriter>(config, options, nameTable, keyColumns, masterChannel, transactionId, parentChunkListId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
