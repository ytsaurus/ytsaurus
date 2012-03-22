#pragma once

#include "common.h"
#include "value.h"
#include "schema.h"
#include "channel_writer.h"
#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/misc/ref_counted.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/error.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IAsyncWriter> TPtr;

    virtual TAsyncError::TPtr AsyncOpen(
        const NProto::TTableChunkAttributes& attributes) = 0;

    virtual TAsyncError::TPtr AsyncEndRow(
        TKey& key,
        std::vector<TChannelWriter::TPtr>& channels) = 0;

    virtual TAsyncError::TPtr AsyncClose(
        TKey& lastKey,
        std::vector<TChannelWriter::TPtr>& channels) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
