#pragma once

#include "static_table_export.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/library/profiling/sensor.h>

#include <map>
#include <span>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline constexpr int StaticTableExportFormatVersion = 11;

struct TStaticTableExportPart
    : public NYTree::TYsonStruct
{
    i64 FirstOffset{};
    i64 NextOffset{};

    REGISTER_YSON_STRUCT(TStaticTableExportPart);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStaticTableExportPart);

//! Durable per-partition state; its attribute key is the ownership and commit lock.
struct TStaticTableExportCursor
    : public NYTree::TYsonStruct
{
    TUniqueSeqNo Generation;
    i64 NextOffset{};
    std::optional<ui64> HighestWindow;
    //! Committed offset ranges of unpublished windows.
    std::map<ui64, TStaticTableExportPartPtr> PendingRanges;

    REGISTER_YSON_STRUCT(TStaticTableExportCursor);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStaticTableExportCursor);

//! Publication state; only the publisher writes it.
struct TStaticTableExportProgress
    : public NYTree::TYsonStruct
{
    int FormatVersion{};
    NYTree::INodePtr Owner;
    std::string SourceIdentity;
    int PartitionCount{};
    //! Fingerprint of the schema of the unpublished gates.
    ui64 SchemaFingerprint{};
    //! Start of the last published window.
    ui64 ClosedThrough{};
    std::vector<i64> PublishedOffsets;

    REGISTER_YSON_STRUCT(TStaticTableExportProgress);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStaticTableExportProgress);

using TStaticTableExportCursors = THashMap<int, TStaticTableExportCursorPtr>;

struct TStaticTableExportClaim
{
    int Partition{};
    TUniqueSeqNo Generation;
};

struct TStaticTableExportDelta
    : public TStaticTableExportClaim
{
    i64 FirstOffset{};
    i64 NextOffset{};
};

struct TStaticTableExportRange
{
    int Partition{};
    i64 FirstOffset{};
    i64 NextOffset{};
};

struct TStaticTableExportPublication
{
    std::string Name;
    NYTree::IAttributeDictionaryPtr Attributes;
};

using TBuildStaticTableExportPublication = std::function<TStaticTableExportPublication(
    ui64 window,
    std::span<const TStaticTableExportRange> ranges)>;

DEFINE_ENUM(EStaticTableExportCommitOutcome,
    (Committed)
    (NotCommitted)
    (Unknown)
);

DEFINE_ENUM(EStaticTableExportPublishResult,
    (Published)
    (Busy)
    (EmptyTail)
);

//! One transaction appending to the shared table of a window.
class TStaticTableExportWindowStream
    : public TRefCounted
{
public:
    TStaticTableExportWindowStream(ui64 window, NApi::ITransactionPtr transaction);
    ~TStaticTableExportWindowStream() override;

private:
    const ui64 Window_;
    const NApi::ITransactionPtr Transaction_;
    std::vector<NApi::ITableWriterPtr> Writers_;
    std::vector<std::vector<int>> IdMappings_;
    std::vector<TInstant> WriterDataTimes_;
    bool Finished_ = false;

    friend class TStaticTableExportWriterStorage;
};

DEFINE_REFCOUNTED_TYPE(TStaticTableExportWindowStream);

//! Cypress layout and transactions of an export; every method blocks the calling fiber.
class TStaticTableExportWriterStorage
    : public TRefCounted
{
public:
    TStaticTableExportWriterStorage(
        NApi::IClientPtr client,
        TStaticTableExportParametersPtr parameters,
        NTableClient::TTableSchemaPtr schema,
        NYTree::INodePtr owner,
        std::string sourceIdentity,
        NTableClient::TTableWriterConfigPtr tableWriterConfig,
        int tableWriterCount,
        const NProfiling::TProfiler& profiler);

    //! Null until the publisher has bootstrapped the export.
    TStaticTableExportProgressPtr ReadProgress();

    //! Raises the generations of |claims|; a returned cursor with a greater generation fences its claim.
    //! Returns null progress and no cursors before bootstrap.
    std::pair<TStaticTableExportProgressPtr, TStaticTableExportCursors> Claim(
        std::span<const TStaticTableExportClaim> claims,
        TDuration timeout);

    //! Reads cursors after their keys are locked, so remote commit effects are visible.
    TStaticTableExportCursors ReadCursors(const std::vector<int>& partitions, TDuration timeout);

    //! Returns null with the current progress when the window gate is published, beyond the band, busy or of another schema.
    std::pair<TStaticTableExportWindowStreamPtr, TStaticTableExportProgressPtr> OpenStream(
        ui64 window,
        TDuration timeout,
        TInstant deadline);
    void WriteStream(
        const TStaticTableExportWindowStreamPtr& stream,
        const NTableClient::TNameTablePtr& nameTable,
        std::span<const TSharedRange<NTableClient::TUnversionedRow>> rowRanges,
        TDuration ioTimeout,
        bool backlogged);
    void CloseStream(const TStaticTableExportWindowStreamPtr& stream, TDuration ioTimeout);
    //! Atomically checks ownership and offsets, advances the cursors and commits the appended rows.
    TStaticTableExportCursors CommitStream(
        const TStaticTableExportWindowStreamPtr& stream,
        std::span<const TStaticTableExportDelta> deltas,
        ui64 closedThrough,
        TDuration lockBackoff);
    void AbortStream(const TStaticTableExportWindowStreamPtr& stream);

    TStaticTableExportProgressPtr Bootstrap(int partitionCount, ui64 closedThrough);
    bool HasCurrentSchema(const TStaticTableExportProgressPtr& progress) const;
    //! Alters the unpublished gates to the current schema; returns false when a writer still holds one.
    bool MigrateSchema();
    //! Publishes the window after the last published one; with |tailOnly| skips it unless some partition wrote it or later.
    EStaticTableExportPublishResult PublishNext(
        const TBuildStaticTableExportPublication& buildPublication,
        bool tailOnly,
        std::optional<TDuration> tableTtl);

private:
    const NApi::IClientPtr Client_;
    const TStaticTableExportParametersPtr Parameters_;
    const NTableClient::TTableSchemaPtr Schema_;
    const ui64 SchemaFingerprint_;
    const NYTree::INodePtr Owner_;
    const std::string SourceIdentity_;
    const NTableClient::TTableWriterConfigPtr TableWriterConfig_;
    const int TableWriterCount_;
    const NProfiling::TEventTimer CommitTimer_;
    const NProfiling::TEventTimer PublicationTimer_;
    const NProfiling::TCounter PublicationConflicts_;

    NYPath::TYPath GetRootPath() const;
    NYPath::TYPath GetPartitionsPath() const;
    NYPath::TYPath GetGatePath(ui64 window) const;

    void ValidateProgress(const TStaticTableExportProgressPtr& progress) const;
    TStaticTableExportProgressPtr ReadProgress(const NApi::IClientBasePtr& client) const;
    TStaticTableExportCursors ReadCursors(
        const NApi::IClientBasePtr& client,
        const std::vector<int>& partitions) const;
    void LockPartitions(
        const NApi::ITransactionPtr& transaction,
        std::vector<int> partitions,
        TInstant deadline,
        TDuration backoff) const;
    void SaveCursors(const NApi::ITransactionPtr& transaction, const TStaticTableExportCursors& cursors) const;
    void CreateGate(const NApi::ITransactionPtr& transaction, ui64 window) const;
    NApi::ITransactionPtr StartTransaction(TDuration timeout, std::optional<TInstant> deadline = {}) const;

    static void WriteRows(
        const NApi::ITableWriterPtr& writer,
        std::vector<int>* idMapping,
        const NTableClient::TNameTablePtr& nameTable,
        std::span<const TSharedRange<NTableClient::TUnversionedRow>> rowRanges,
        TDuration ioTimeout);
};

DEFINE_REFCOUNTED_TYPE(TStaticTableExportWriterStorage);

//! Tells whether a delta starting at |offset| continues the durable prefix of |cursor|.
bool IsStaticTableExportCursorAt(const TStaticTableExportCursor& cursor, i64 offset);

//! Tells from locked cursors whether a stream commit with |deltas| into |window| happened.
//! Partitions owned by a newer job are returned in |fenced| and excluded from the decision.
EStaticTableExportCommitOutcome ClassifyStaticTableExportCommit(
    ui64 window,
    std::span<const TStaticTableExportDelta> deltas,
    const TStaticTableExportCursors& cursors,
    std::vector<int>* fenced);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
