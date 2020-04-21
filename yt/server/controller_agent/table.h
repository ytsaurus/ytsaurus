#pragma once

#include "helpers.h"
#include "private.h"
#include "table.h"

#include <yt/server/lib/chunk_pools/chunk_stripe_key.h>

#include <yt/server/lib/controller_agent/serialize.h>

#include <yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/client/table_client/column_rename_descriptor.h>
#include <yt/ytlib/table_client/table_upload_options.h>

namespace NYT::NControllerAgent {

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
    NCypressClient::TNodeId LivePreviewTableId;

    void Persist(const TPersistenceContext& context);
};

struct TInputTable
    : public TIntrinsicRefCounted
    , public NChunkClient::TUserObject
{
    using NChunkClient::TUserObject::TUserObject;

    //! Number of chunks in the whole table (without range selectors).
    std::vector<NChunkClient::TInputChunkPtr> Chunks;
    NTableClient::TColumnRenameDescriptors ColumnRenameDescriptors;
    NTableClient::TTableSchema Schema;
    NTableClient::ETableSchemaMode SchemaMode;
    bool Dynamic = false;
    NHydra::TRevision ContentRevision = NHydra::NullRevision;

    //! Set to true when schema of the table is compatible with the output
    //! teleport table and when no special options set that disallow chunk
    //! teleporting (like force_transform = %true).
    bool Teleportable = false;

    bool IsForeign() const;
    bool IsPrimary() const;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TInputTable)

struct TOutputTable
    : public NChunkClient::TUserObject
    , public TLivePreviewTableBase
    , public TIntrinsicRefCounted
{
    TOutputTable() = default;
    TOutputTable(
        NYPath::TRichYPath path,
        EOutputTableType outputType);

    NTableClient::TTableWriterOptionsPtr TableWriterOptions = New<NTableClient::TTableWriterOptions>();
    NTableClient::TTableUploadOptions TableUploadOptions;
    EOutputTableType OutputType = EOutputTableType::Output;

    // Upload transaction id for the native and external cell.
    NTransactionClient::TTransactionId UploadTransactionId;

    // Chunk list for appending the output.
    NChunkClient::TChunkListId OutputChunkListId;

    // Last key of the table for checking sort order.
    NTableClient::TOwningKey LastKey;

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

    NTransactionClient::TTimestamp Timestamp = NTransactionClient::NullTimestamp;

    //! Corresponding sink.
    NChunkPools::IChunkPoolInput* ChunkPoolInput = nullptr;

    bool Dynamic = false;
    std::vector<NTableClient::TOwningKey> PivotKeys;
    std::vector<NChunkClient::TChunkListId> TabletChunkListIds;

    std::vector<NChunkClient::TInputChunkPtr> OutputChunks;

    TEdgeDescriptor GetEdgeDescriptorTemplate(int tableIndex = -1);

    bool IsBeginUploadCompleted() const;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TOutputTable)

struct TIntermediateTable
    : public TLivePreviewTableBase
    , public TIntrinsicRefCounted
{
    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TIntermediateTable)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

