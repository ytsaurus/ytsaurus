#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/client/chunk_client/config.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderConfig
    : public virtual NChunkClient::TReplicationReaderConfig
{
public:
    //! Reader will skip replicas with less than this amount of relevant data
    //! data available.
    i64 ReplicaDataSizeReadThreshold;

    TChunkReaderConfig()
    {
        RegisterParameter("replica_data_size_read_threshold", ReplicaDataSizeReadThreshold)
            .Default(1_MB);
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
