#include "stdafx.h"
#include "table_chunk_sequence_reader.h"
#include "config.h"
#include "schema.h"

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/ytree/convert.h>

#include <limits>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = TableReaderLogger;

////////////////////////////////////////////////////////////////////////////////

TTableChunkSequenceReader::TTableChunkSequenceReader(
    TChunkSequenceReaderConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NChunkClient::IBlockCachePtr blockCache,
    std::vector<NProto::TInputChunk>&& inputChunks,
    const TReaderOptions& options)
    : TChunkSequenceReaderBase<TTableChunkReader>(config, masterChannel, blockCache, MoveRV(inputChunks), Logger)
    , Options(options)
    , ValueCount_(0)
    , RowCount_(0)
{
    for (int i = 0; i < static_cast<int>(InputChunks.size()); ++i) {
        auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(InputChunks[i].extensions());
        RowCount_ += InputChunks[i].row_count();
        ValueCount_ += miscExt.value_count();
    }
}

TTableChunkReaderPtr TTableChunkSequenceReader::CreateNewReader(
    const NProto::TInputChunk& inputChunk, 
    NChunkClient::IAsyncReaderPtr asyncReader)
{
    const auto& slice = inputChunk.slice();
    return New<TTableChunkReader>(
        Config->SequentialReader,
        TChannel::FromProto(inputChunk.channel()),
        asyncReader,
        slice.start_limit(),
        slice.end_limit(),
        // TODO(ignat) yson type ?
        NYTree::TYsonString(inputChunk.row_attributes()),
        inputChunk.partition_tag(),
        Options); // ToDo(psushin): pass row attributes here.
}

void TTableChunkSequenceReader::OnChunkSwitch(const TReaderPtr& nextReader)
{
    RowCount_ = GetItemIndex() + nextReader->GetRowCount();
    for (int i = CurrentReaderIndex + 1; i < InputChunks.size(); ++i) {
        RowCount_ += InputChunks[i].row_count();
    }
}

double TTableChunkSequenceReader::GetProgress() const
{
    return GetItemIndex() / double(GetRowCount());
}

bool TTableChunkSequenceReader::KeepReaders() const
{
    return Options.KeepBlocks;
}

const TRow& TTableChunkSequenceReader::GetRow() const
{
    return CurrentReader_->GetRow();
}

const TNonOwningKey& TTableChunkSequenceReader::GetKey() const
{
    return CurrentReader_->GetKey();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
