#pragma once

#include "private.h"

#include "data_flow_graph.h"

#include <yt/yt/server/controller_agent/helpers.h>

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe_key.h>

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/client/table_client/column_rename_descriptor.h>
#include <yt/yt/ytlib/table_client/table_upload_options.h>

namespace NYT::NControllerAgent::NControllers {

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

// Base class for TInputTable and TOutputTable.
struct TTableBase
    : public NChunkClient::TUserObject
{
    using NChunkClient::TUserObject::TUserObject;

    NTableClient::TTableSchemaPtr Schema = New<NTableClient::TTableSchema>();
    TGuid SchemaId;

    void Persist(const TPersistenceContext& context);
};

struct TInputTable
    : public TRefCounted
    , public TTableBase
{
    using TTableBase::TTableBase;

    //! Number of chunks in the whole table (without range selectors).
    std::vector<NChunkClient::TInputChunkPtr> Chunks;
    NTableClient::TColumnRenameDescriptors ColumnRenameDescriptors;
    //! Comparator corresponding to the input table sort order.
    //! Used around read limits using keys.
    NTableClient::TComparator Comparator;
    NTableClient::ETableSchemaMode SchemaMode;
    bool Dynamic = false;
    NHydra::TRevision ContentRevision = NHydra::NullRevision;

    //! Set to true when schema of the table is compatible with the output
    //! teleport table and when no special options set that disallow chunk
    //! teleporting (like force_transform = %true).
    bool Teleportable = false;

    bool IsForeign() const;
    bool IsPrimary() const;

    //! Returns true unless teleportation is forbidden by some table options,
    //! e.g. dynamism or renamed columns.
    //! NB: this method depends only on internal table properties. Use
    //! |Teleportable| to get effective value.
    bool SupportsTeleportation() const;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TInputTable)

struct TOutputTable
    : public TTableBase
    , public TLivePreviewTableBase
    , public TRefCounted
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
    NTableClient::TKey LastKey;

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

    bool Dynamic = false;
    std::vector<NTableClient::TLegacyOwningKey> PivotKeys;
    std::vector<NChunkClient::TChunkListId> TabletChunkListIds;

    std::vector<NChunkClient::TInputChunkPtr> OutputChunks;

    int TableIndex;

    TOutputStreamDescriptorPtr GetStreamDescriptorTemplate(int tableIndex = -1);

    bool IsBeginUploadCompleted() const;

    bool SupportsTeleportation() const;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TOutputTable)

struct TIntermediateTable
    : public TLivePreviewTableBase
    , public TRefCounted
{
    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TIntermediateTable)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
