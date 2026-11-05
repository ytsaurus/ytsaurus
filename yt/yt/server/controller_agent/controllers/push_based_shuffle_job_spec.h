#pragma once

#include "private.h"

#include <yt/yt/server/controller_agent/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/session_pool.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/compression/public.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TPushBasedShuffleJobSpecBuilder
    : public TRefCounted
{
public:
    TPushBasedShuffleJobSpecBuilder(
        TPushBasedShuffleOptionsPtr options,
        NTableClient::TTableSchemaPtr intermediateStreamSchema,
        NCompression::ECodec codec,
        int replicationFactor,
        int partitionCount,
        i64 writerMaxBufferSize);

    void ValidateWriterMemoryBudget() const;
    i64 ComputeWriterJobMemorySize() const;

    void FillWriterSpecTemplate(NProto::TJobSpec* jobSpecTemplate) const;
    static void FillWriterSpec(
        NProto::TJobSpec* jobSpec,
        int taskJobIndex,
        const std::vector<NDistributedChunkSessionClient::TReadySession>& readySessions);

    void FillSortReaderSpec(NProto::TPushBasedShuffleSortReaderSpec* readerSpec) const;
    static void FillValidTaskJobIndexes(NProto::TJobSpec* jobSpec, const std::vector<int>& taskJobIndexes);

private:
    const TPushBasedShuffleOptionsPtr Options_;
    const NTableClient::TTableSchemaPtr IntermediateStreamSchema_;
    const NCompression::ECodec Codec_;
    const int ReplicationFactor_;
    const int PartitionCount_;
    const i64 WriterMaxBufferSize_;

    i64 ComputeWriterMetadataSize() const;
    i64 ComputeWriterMemoryBudget() const;
};

DEFINE_REFCOUNTED_TYPE(TPushBasedShuffleJobSpecBuilder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
