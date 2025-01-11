#pragma once

#include "private.h"

#include "data_flow_graph.h"

#include <yt/yt/server/controller_agent/helpers.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe_key.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/client/table_client/column_rename_descriptor.h>
#include <yt/yt/client/table_client/table_upload_options.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

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
    TString LivePreviewTableName;

    PHOENIX_DECLARE_TYPE(TLivePreviewTableBase, 0xcc71c8d6);
};

// Base class for TInputTable and TOutputTable.
struct TTableBase
    : public NChunkClient::TUserObject
{
    using NChunkClient::TUserObject::TUserObject;

    NTableClient::TTableSchemaPtr Schema = New<NTableClient::TTableSchema>();
    TGuid SchemaId;

    bool IsFile() const;

    PHOENIX_DECLARE_TYPE(TTableBase, 0xc863d38f);
};

struct TInputTable
    : public TRefCounted
    , public TTableBase
{
    using TTableBase::TTableBase;

    //! Number of chunks in the whole table (without range selectors).
    std::vector<NChunkClient::TInputChunkPtr> Chunks;
    std::vector<NChunkClient::TInputChunkPtr> HunkChunks;
    NTableClient::TColumnRenameDescriptors ColumnRenameDescriptors;
    //! Comparator corresponding to the input table sort order.
    //! Used around read limits using keys.
    NTableClient::TComparator Comparator;
    NTableClient::ETableSchemaMode SchemaMode;
    bool Dynamic = false;
    NHydra::TRevision ContentRevision = NHydra::NullRevision;

    NScheduler::TClusterName ClusterName;

    //! Set to true when schema of the table is compatible with the output
    //! teleport table and when no special options set that disallow chunk
    //! teleporting (like force_transform = %true).
    bool Teleportable = false;

    bool IsForeign() const;
    bool IsPrimary() const;
    bool IsVersioned() const;
    bool UseReadViaExecNode() const;

    //! Returns true unless teleportation is forbidden by some table options,
    //! e.g. dynamism, renamed columns or hunk columns.
    //! NB: This method depends only on internal table properties. Use
    //! |Teleportable| to get effective value.
    bool SupportsTeleportation() const;

    PHOENIX_DECLARE_TYPE(TInputTable, 0xd9d35083);
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
    NChunkClient::TChunkListId OutputHunkChunkListId;

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
    std::vector<std::pair<NChunkPools::TChunkStripeKey, NChunkClient::TChunkTreeId>> OutputHunkChunkTreeIds;

    NYson::TYsonString EffectiveAcl;

    NYson::TYsonString WriterConfig;

    NTransactionClient::TTimestamp Timestamp = NTransactionClient::NullTimestamp;

    bool Dynamic = false;
    std::vector<NTableClient::TLegacyOwningKey> PivotKeys;
    std::vector<NChunkClient::TChunkListId> TabletChunkListIds;
    std::vector<NChunkClient::TChunkListId> TabletHunkChunkListIds;

    std::vector<NChunkClient::TInputChunkPtr> OutputChunks;
    std::vector<NChunkClient::TInputChunkPtr> OutputHunkChunks;

    int TableIndex;

    TOutputStreamDescriptorPtr GetStreamDescriptorTemplate(int tableIndex = -1);

    bool IsBeginUploadCompleted() const;

    bool SupportsTeleportation() const;

    bool IsDebugTable() const;

    PHOENIX_DECLARE_TYPE(TOutputTable, 0x56fd1f6b);
};

DEFINE_REFCOUNTED_TYPE(TOutputTable)

struct TIntermediateTable
    : public TLivePreviewTableBase
    , public TRefCounted
{
    PHOENIX_DECLARE_TYPE(TIntermediateTable, 0x1a4e35b3);
};

DEFINE_REFCOUNTED_TYPE(TIntermediateTable)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
