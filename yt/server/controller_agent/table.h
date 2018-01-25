#pragma once

#include "helpers.h"
#include "private.h"
#include "serialize.h"
#include "table.h"

#include <yt/server/chunk_pools/chunk_stripe_key.h>

#include <yt/ytlib/chunk_client/data_statistics.pb.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/table_client/helpers.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOutputTableType,
    (Output)
    (Stderr)
    (Core)
);

////////////////////////////////////////////////////////////////////////////////

struct TLivePreviewTableBase
{
    // Live preview table ids.
    std::vector<NCypressClient::TNodeId> LivePreviewTableIds;

    void Persist(const TPersistenceContext& context);
};

struct TInputTable
    : public TLockedUserObject
{
    //! Number of chunks in the whole table (without range selectors).
    int ChunkCount = -1;
    std::vector<NChunkClient::TInputChunkPtr> Chunks;
    NTableClient::TTableSchema Schema;
    NTableClient::ETableSchemaMode SchemaMode;
    bool IsDynamic;

    //! Set to true when schema of the table is compatible with the output
    //! teleport table and when no special options set that disallow chunk
    //! teleporting (like force_transform = %true).
    bool IsTeleportable = false;

    bool IsForeign() const;

    bool IsPrimary() const;

    void Persist(const TPersistenceContext& context);
};

struct TOutputTable
    : public NChunkClient::TUserObject
    , public TLivePreviewTableBase
{
    NTableClient::TTableWriterOptionsPtr Options = New<NTableClient::TTableWriterOptions>();
    NTableClient::TTableUploadOptions TableUploadOptions;
    EOutputTableType OutputType = EOutputTableType::Output;

    // Server-side upload transaction.
    NTransactionClient::TTransactionId UploadTransactionId;

    // Chunk list for appending the output.
    NChunkClient::TChunkListId OutputChunkListId;

    // Statistics returned by EndUpload call.
    NChunkClient::NProto::TDataStatistics DataStatistics;

    // TODO(max42): move this and other runtime-specific data to TOperationControllerBase::TSink.
    //! Chunk trees comprising the output (the order matters).
    //! Chunk trees are sorted according to either:
    //! * integer key (e.g. in remote copy);
    //! * boundary keys (when the output is sorted).
    std::vector<std::pair<NChunkPools::TChunkStripeKey, NChunkClient::TChunkTreeId>> OutputChunkTreeIds;

    NYson::TYsonString EffectiveAcl;

    NYson::TYsonString WriterConfig;

    NTransactionClient::TTimestamp Timestamp;

    TEdgeDescriptor GetEdgeDescriptorTemplate();

    bool IsBeginUploadCompleted() const;

    void Persist(const TPersistenceContext& context);
};

struct TIntermediateTable
    : public TLivePreviewTableBase
{
    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

