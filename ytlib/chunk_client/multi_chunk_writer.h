#pragma once

#include "public.h"
#include "chunk_replica.h"
#include "data_statistics.h"
#include "writer_base.h"

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IMultiChunkWriter
    : public virtual IWriterBase
{
    virtual const std::vector<NProto::TChunkSpec>& GetWrittenChunksMasterMeta() const = 0;

    virtual const std::vector<NProto::TChunkSpec>& GetWrittenChunksFullMeta() const = 0;

    virtual NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const = 0;

    virtual NProto::TDataStatistics GetDataStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
