#pragma once

#include "public.h"
#include "value.h"
#include "schema.h"
#include "channel_writer.h"

#include <ytlib/chunk_holder/chunk.pb.h>

#include <ytlib/misc/ref_counted.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::map<TStringBuf, TStringBuf> TRow;

struct IAsyncWriter
    : public virtual TRefCounted
{
    virtual TAsyncError AsyncOpen() = 0;
    virtual TAsyncError AsyncWriteRow(const TRow& row) = 0;
    virtual TAsyncError AsyncSwitchChunk(const NChunkClient::NProto::TChunkMeta& chunkMeta) = 0;
    virtual TAsyncError AsyncClose(const NChunkClient::NProto::TChunkMeta& chunkMeta) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
