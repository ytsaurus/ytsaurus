#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/writer_base.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

using TChunkWithReplicasList = std::vector<std::pair<TChunkId, TChunkReplicaWithLocationList>>;

////////////////////////////////////////////////////////////////////////////////

struct IMultiChunkWriter
    : public virtual IWriterBase
{
    virtual const std::vector<NProto::TChunkSpec>& GetWrittenChunkSpecs() const = 0;
    virtual const TChunkWithReplicasList& GetWrittenChunkWithReplicasList() const = 0;

    virtual NProto::TDataStatistics GetDataStatistics() const = 0;
    virtual TCodecStatistics GetCompressionStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
