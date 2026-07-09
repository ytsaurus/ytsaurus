#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

struct IPartitionStates
    : public TRefCounted
{
    struct TTableKey
    {
        TPartitionId PartitionId;
        std::string Name;

        bool operator==(const TTableKey& other) const = default;
    };

    struct TTableKeyFilter
    {
        std::optional<TPartitionId> PartitionId;
        std::optional<std::string> Name;
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

DEFINE_REFCOUNTED_TYPE(IPartitionStates);

////////////////////////////////////////////////////////////////////////////////

class TPartitionStates
    : public IPartitionStates
{
public:
    TPartitionStates(TContextPtr context, TDynamicTableRequestSpecPtr dynamicSpec, std::optional<std::string> defaultTag = std::nullopt);

    void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) override;

    using IPartitionStates::Lookup;
    TFuture<THashMap<TTableKey, NYsonSerializer::TStatePtr>> Lookup(
        THashSet<TTableKey> keys,
        std::optional<std::string> tag = std::nullopt) override;

    using IPartitionStates::Write;
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

DEFINE_REFCOUNTED_TYPE(TPartitionStates);

////////////////////////////////////////////////////////////////////////////////

// Lightweight wrapper around IPartitionStates that automatically supplies a fixed tag
// to all Lookup/Write calls. Allows one shared TPartitionStates per computation while
// each client passes its own tag for throttling/profiling purposes.
class TTaggedPartitionStates
    : public IPartitionStates
{
public:
    TTaggedPartitionStates(IPartitionStatesPtr table, std::string tag);

    void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) override;

    using IPartitionStates::Lookup;
    TFuture<THashMap<TTableKey, NYsonSerializer::TStatePtr>> Lookup(
        THashSet<TTableKey> keys,
        std::optional<std::string> tag = std::nullopt) override;

    using IPartitionStates::Write;
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
    const IPartitionStatesPtr Table_;
    const std::string Tag_;
};

DEFINE_REFCOUNTED_TYPE(TTaggedPartitionStates);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables

template <>
struct THash<NYT::NFlow::NTables::IPartitionStates::TTableKey>
{
    size_t operator()(const NYT::NFlow::NTables::IPartitionStates::TTableKey& tableKey) const;
};
