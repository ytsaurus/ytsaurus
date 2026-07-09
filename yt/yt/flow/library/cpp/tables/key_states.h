#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

struct IKeyStates
    : public TRefCounted
{
    struct TTableKey
    {
        TComputationId ComputationId;
        TKey Key;
        std::string Name;

        bool operator==(const TTableKey& other) const = default;
    };

    struct TTableKeyFilter
    {
        std::optional<TComputationId> ComputationId;
        std::optional<TKey> ExactKey;
        std::optional<TKey> LowerKey;
        std::optional<TKey> UpperKey;
        std::optional<std::string> Name;
        //! When set, replaces `Name` with an `IN (...)` filter so a single
        //! query can stream rows for several state names. Empty vector
        //! degenerates to "match no rows".
        std::optional<std::vector<std::string>> Names;
    };

    struct TListResult
    {
        std::vector<TTableKey> Keys;
        std::optional<TTableKey> ContinuationOffsetExclusive;
    };

    virtual void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) = 0;

    virtual TFuture<THashMap<TTableKey, NYsonSerializer::TStatePtr>> Lookup(
        THashSet<TTableKey> keys,
        std::optional<std::string> tag = std::nullopt) = 0;

    virtual void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const THashMap<TTableKey, NYsonSerializer::TStateMutation>& mutations,
        std::optional<std::string> tag = std::nullopt) = 0;

    virtual TFuture<TListResult> List(
        TTableKeyFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) = 0;

    virtual TFuture<std::vector<TTableKey>> ListAll(TTableKeyFilter filter) = 0;

    // Non-virtual convenience methods.
    TFuture<NYsonSerializer::TStatePtr> Lookup(TTableKey key, std::optional<std::string> tag = std::nullopt);

    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const TTableKey& key,
        const NYsonSerializer::TStateMutation& mutation,
        std::optional<std::string> tag = std::nullopt);

    void Erase(
        NApi::IDynamicTableTransactionPtr transaction,
        const THashSet<TTableKey>& keys);

    void Erase(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTableKey>& keys);
};

DEFINE_REFCOUNTED_TYPE(IKeyStates);

////////////////////////////////////////////////////////////////////////////////

class TKeyStates
    : public IKeyStates
{
public:
    TKeyStates(TContextPtr context, TDynamicTableRequestSpecPtr dynamicSpec);

    void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) override;

    using IKeyStates::Lookup;
    TFuture<THashMap<TTableKey, NYsonSerializer::TStatePtr>> Lookup(
        THashSet<TTableKey> keys,
        std::optional<std::string> tag = std::nullopt) override;

    using IKeyStates::Write;
    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const THashMap<TTableKey, NYsonSerializer::TStateMutation>& mutations,
        std::optional<std::string> tag = std::nullopt) override;

    TFuture<TListResult> List(
        TTableKeyFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) override;

    TFuture<std::vector<TTableKey>> ListAll(TTableKeyFilter filter) override;

private:
    // Per-tag metrics for lookup/write operations.
    struct TTagMetrics
    {
        NProfiling::TCounter LookupRows;
        NProfiling::TCounter LookupBytes;
        NProfiling::TCounter SelectRows;
        NProfiling::TCounter SelectBytes;
        NProfiling::TCounter WriteRows;
        NProfiling::TCounter WriteBytes;
        NProfiling::TCounter UpdateRows;
        NProfiling::TCounter EraseRows;
        NProfiling::TEventTimer LookupTime;
        NProfiling::TEventTimer SelectTime;
    };

    TTagMetrics& GetOrCreateTagMetrics(const std::string& tag);

    const TContextPtr Context_;
    TDynamicTableRequestSpecPtr DynamicSpec_;
    const NYPath::TYPath TablePath_;
    const NLogging::TLogger Logger;
    const std::string DefaultTag_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, TagMetricsLock_);
    THashMap<std::string, TTagMetrics> TagMetrics_;
};

DEFINE_REFCOUNTED_TYPE(TKeyStates);

////////////////////////////////////////////////////////////////////////////////

// Lightweight wrapper around IKeyStates that automatically supplies a fixed tag
// to all Lookup/Write calls. Allows one shared TKeyStates per computation while
// each client passes its own tag for throttling/profiling purposes.
class TTaggedKeyStates
    : public IKeyStates
{
public:
    TTaggedKeyStates(IKeyStatesPtr table, std::string tag);

    void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) override;

    using IKeyStates::Lookup;
    TFuture<THashMap<TTableKey, NYsonSerializer::TStatePtr>> Lookup(
        THashSet<TTableKey> keys,
        std::optional<std::string> tag = std::nullopt) override;

    using IKeyStates::Write;
    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const THashMap<TTableKey, NYsonSerializer::TStateMutation>& mutations,
        std::optional<std::string> tag = std::nullopt) override;

    TFuture<TListResult> List(
        TTableKeyFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) override;

    TFuture<std::vector<TTableKey>> ListAll(TTableKeyFilter filter) override;

private:
    const IKeyStatesPtr Table_;
    const std::string Tag_;
};

DEFINE_REFCOUNTED_TYPE(TTaggedKeyStates);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables

template <>
struct THash<NYT::NFlow::NTables::IKeyStates::TTableKey>
{
    size_t operator()(const NYT::NFlow::NTables::IKeyStates::TTableKey& tableKey) const;
};
