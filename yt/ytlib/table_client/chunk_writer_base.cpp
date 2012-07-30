#include "stdafx.h"
#include "chunk_writer_base.h"
#include "config.h"
#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/private.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/chunk_client/async_writer.h>
#include <ytlib/chunk_client/encoding_writer.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TChunkWriterBase::TChunkWriterBase(
    NChunkClient::IAsyncWriterPtr chunkWriter,
    TChunkWriterConfigPtr config)
    : Config(config)
    , ChunkWriter(chunkWriter)
    , EncodingWriter(New<TEncodingWriter>(Config, chunkWriter))
    , CurrentBlockIndex(0)
    , DataWeight(0)
    , RowCount(0)
    , ValueCount(0)
    , CurrentSize(0)
{
    VERIFY_INVOKER_AFFINITY(WriterThread->GetInvoker(), WriterThread);
}

void TChunkWriterBase::FinalizeWriter()
{
    Meta.set_type(EChunkType::Table);

    SetProtoExtension(Meta.mutable_extensions(), ChannelsExt);

    {
        MiscExt.set_uncompressed_data_size(EncodingWriter->GetUncompressedSize());
        MiscExt.set_compressed_data_size(EncodingWriter->GetCompressedSize());
        MiscExt.set_meta_size(Meta.ByteSize());
        MiscExt.set_codec_id(Config->CodecId);
        MiscExt.set_data_weight(DataWeight);
        MiscExt.set_row_count(RowCount);
        MiscExt.set_value_count(ValueCount);
        SetProtoExtension(Meta.mutable_extensions(), MiscExt);
    }

    auto this_ = MakeStrong(this);
    ChunkWriter->AsyncClose(Meta).Subscribe(BIND([=] (TError error) {
        // ToDo(psushin): more verbose diagnostic.
        this_->State.Finish(error);
    }));
}

TAsyncError TChunkWriterBase::GetReadyEvent()
{
    State.StartOperation();

    auto this_ = MakeStrong(this);
    EncodingWriter->GetReadyEvent().Subscribe(BIND([=](TError error){
        this_->State.FinishOperation(error);
    }));

    return State.GetOperationError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
