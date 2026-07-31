#pragma once

#include "external_state_manager_base.h"
#include "public.h"
#include "simple_external_state_manager.h"

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/library/profiling/sensor.h>

#include <deque>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnavailableSourcePolicy,
    (Retry)
    (MarkUnreadable)
);

////////////////////////////////////////////////////////////////////////////////

struct TStaticTableKeyVisitorJoinerSpec
    : public IExternalStateJoiner::TParameters
{
    //! The external static sorted source table. Its key-prefix columns must hold the same values
    //! as the computation's group-by key columns (e.g. the partition column must equal
    //! ``farm_hash(key)``); a source materialized with mismatched values has every visit key
    //! silently resolve as absent.
    NYPath::TRichYPath Path;
    //! ``retry``: a failed source read fails the fill iteration and is retried; the sweep does
    //! not advance past the failed range. ``mark_unreadable``: the failure is swallowed, the
    //! sweep runs forward, and the failed range's keys resolve as unreadable (an uninitialized
    //! accessor).
    EUnavailableSourcePolicy UnavailableSourcePolicy{};

    REGISTER_YSON_STRUCT(TStaticTableKeyVisitorJoinerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TStaticTableKeyVisitorJoinerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicStaticTableKeyVisitorJoinerSpec
    : public IExternalStateJoiner::TDynamicParameters
{
    //! Attempt budget for one source read; a read that exhausts it fails and is handled per
    //! ``unavailable_source_policy``.
    int ReadAttempts{};
    //! For how long a failed source read marks the source unavailable. While marked, reads are
    //! not attempted: every #List resolves immediately per ``unavailable_source_policy``; the
    //! first #List past the mark probes the source again.
    TDuration UnavailableSourceBackoff;

    REGISTER_YSON_STRUCT(TDynamicStaticTableKeyVisitorJoinerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicStaticTableKeyVisitorJoinerSpec);

////////////////////////////////////////////////////////////////////////////////

//! A joiner that traverses a static YT table sequentially (sorted like the computation's
//! group-by key) alongside the key-visitor's own internal-state traversal, exposing each
//! row to the computation as a read-only per-key state.
//!
//! Unlike #TSimpleExternalStateJoiner it never does random access: the key-visitor fill loop
//! calls #List over each swept range, and the visitor-driven preload promotes the visited
//! keys into per-epoch states. A visited key absent from the table yields an empty state.
class TStaticTableKeyVisitorJoiner
    : public TExternalStateJoinerBase<TSimpleExternalState>
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TStaticTableKeyVisitorJoinerSpec);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicStaticTableKeyVisitorJoinerSpec);

    static constexpr bool VisitorDriven = true;

    using TStateSchemaPtr = NTableClient::TTableSchemaPtr;

    TStaticTableKeyVisitorJoiner(
        TExternalStateJoinerContextPtr context,
        TDynamicExternalStateJoinerContextPtr dynamicContext);

    bool IsVisitorDriven() const final;

    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) final;
    void Reset() final;

    IStateHolderPtr GetState(const TKey& key) final;

    TFuture<TListResult> List(TFilter filter, i64 limit, std::optional<TKey> offsetExclusive) final;

protected:
    //! Result of one sequential range read: row i is Keys[i] with payload Payloads[i], both
    //! described by StateSchema.
    struct TListedStates
    {
        std::vector<TKey> Keys;
        TStateSchemaPtr StateSchema;
        std::vector<TPayload> Payloads;
    };

    //! Sequential read of ``[filter.LowerKey, filter.UpperKey)`` returning key and payload
    //! columns in key order, stopping after |limit| rows. Overridable so tests can script
    //! the read without a real YT client.
    virtual TFuture<TListedStates> DoListStates(TFilter filter, i64 limit);

    struct TListedRow
    {
        TPayload Payload;
        TStateSchemaPtr Schema;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    THashMap<TKey, TListedRow> Listed_;
    THashMap<TKey, TStateHolderPtr> States_;
    std::vector<TKeyRange> ListedRanges_;

private:
    const EUnavailableSourcePolicy UnavailableSourcePolicy_;
    const TStateSchemaPtr KeySchema_;
    const NApi::IClientPtr Client_;
    const NYPath::TRichYPath Path_;
    const NLogging::TLogger Logger;
    const NProfiling::TGauge ListedSizeGauge_;
    const NProfiling::TCounter ReaderOpensCounter_;
    //! 1 while the source is gated after a failed read, 0 while it is readable.
    const NProfiling::TGauge SourceUnavailableGauge_;
    //! Failed source reads; the immediate resolves served from the gate do not count.
    const NProfiling::TCounter FailedReadsCounter_;

    TInstant NextAttemptTime_;
    TError LastSourceError_;

    struct TColumnTarget
    {
        bool IsKey = false;
        int Position = 0;
    };

    struct TSchemaBundle
    {
        TStateSchemaPtr StateSchema;
        int KeyCount = 0;
        std::vector<std::string> ColumnNames;
        THashMap<std::string, TColumnTarget> TargetByColumnName;
    };

    struct TBufferedRow
    {
        TKey Key;
        TPayload Payload;
    };

    std::optional<TSchemaBundle> SchemaBundle_;
    NApi::ITableReaderPtr Reader_;
    bool ReaderExhausted_ = false;
    std::deque<TBufferedRow> Overshoot_;
    std::optional<TKey> ServableFrom_;
    bool ServableInclusive_ = false;

    //! Whether |key| falls in a range whose most recent list attempt succeeded.
    bool IsInListedRange(const TKey& key) const;
    //! Adds |range| to #ListedRanges_, keeping the stored ranges disjoint.
    void AddListedRange(const TKeyRange& range);
    //! Applies #EUnavailableSourcePolicy to a failed list over ``[lower, upper)``: rethrows
    //! |error| under ``retry``, retracts the range's coverage and returns empty otherwise.
    TListResult HandleFailedList(TError error, const TKey& lower, const TKey& upper);

    //! Reads ``@schema``, checks the table is static, validates the schema against the group-by
    //! key, and derives the cached layout.
    TSchemaBundle FetchSchemaBundle();
    //! Opens a forward reader over ``[lower, max)`` with the bundle's columns pinned.
    NApi::ITableReaderPtr OpenForwardReader(const TKey& lower, const TSchemaBundle& bundle);
    //! Decodes a row, placing each value by its name-table column name.
    static TBufferedRow DecodeRow(
        NTableClient::TUnversionedRow row,
        const TSchemaBundle& bundle,
        const NTableClient::TNameTablePtr& nameTable);
};

DEFINE_REFCOUNTED_TYPE(TStaticTableKeyVisitorJoiner);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
