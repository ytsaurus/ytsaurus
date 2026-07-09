#pragma once

#include "external_state_manager_base.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>

#include <yt/yt/client/api/dynamic_table_client.h>
#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/ypath/rich.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TSimpleExternalState
    : public ICustomStateOps
    , public ICustomYsonView
{
    TPayload Payload;
    NTableClient::TTableSchemaPtr Schema;

    template <class T>
    T GetColumnValue(TStringBuf columnName) const;
    template <class T>
    T GetColumnValue(int columnId) const;

    NTableClient::TUnversionedValue GetColumn(TStringBuf columnName) const;
    NTableClient::TUnversionedValue GetColumn(int columnId) const;

    void Clear() override;
    bool IsEmpty() const override;

    NYson::TYsonString ToYsonView() const override;
};

////////////////////////////////////////////////////////////////////////////////

namespace NSimpleExternalState {

////////////////////////////////////////////////////////////////////////////////

struct TLoadedStates
{
    NTableClient::TTableSchemaPtr StateSchema;
    std::vector<TPayload> Payloads;
};

class TOperator
{
public:
    TOperator(
        NTableClient::TTableSchemaPtr keySchema,
        NApi::IClientBasePtr client,
        NYPath::TYPath path);

    //! Reads |keys| from the state table. If |expectedStateSchema| is non-null,
    //! the lookup throws on a state-schema mismatch.
    TFuture<TLoadedStates> Lookup(
        TRange<TKey> keys,
        NTableClient::TTableSchemaPtr expectedStateSchema) const;

    //! Diffs |oldPayloads| vs |newPayloads| and stages writes/deletes into |tx|.
    //! Both maps must share keys; payloads must follow |stateSchema|.
    void Write(
        const IRetryableTransactionPtr& tx,
        const NTableClient::TTableSchemaPtr& stateSchema,
        const THashMap<TKey, TPayload>& oldPayloads,
        const THashMap<TKey, TPayload>& newPayloads) const;

    struct TListedKeys
    {
        std::vector<TKey> Keys;
        std::optional<TKey> OffsetExclusive;
    };

    TFuture<TListedKeys> ListKeys(
        std::optional<TKey> lowerKey,
        std::optional<TKey> upperKey,
        std::optional<TKey> offsetExclusive,
        i64 limit) const;

private:
    const NTableClient::TTableSchemaPtr KeySchema_;
    const NApi::IClientBasePtr Client_;
    const NYPath::TYPath Path_;
};

////////////////////////////////////////////////////////////////////////////////

struct TCachedValue
    : public IStateCacheValue
{
    TPayload Payload;
    NTableClient::TTableSchemaPtr Schema;

    void Compress() override;
    void Decompress() override;
    i64 GetWeight() override;
};

using TCachedValuePtr = TIntrusivePtr<TCachedValue>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSimpleExternalState

////////////////////////////////////////////////////////////////////////////////

struct TSimpleExternalStateManagerSpec
    : public IExternalStateManager::TParameters
{
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TSimpleExternalStateManagerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSimpleExternalStateManagerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSimpleExternalStateManagerSpec
    : public IExternalStateManager::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TDynamicSimpleExternalStateManagerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSimpleExternalStateManagerSpec);

////////////////////////////////////////////////////////////////////////////////

class TSimpleExternalStateManager
    : public TExternalStateManagerBase<TSimpleExternalState>
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TSimpleExternalStateManagerSpec);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicSimpleExternalStateManagerSpec);

    using TStateSchemaPtr = NTableClient::TTableSchemaPtr;

    TSimpleExternalStateManager(
        TExternalStateManagerContextPtr context,
        TDynamicExternalStateManagerContextPtr dynamicContext);

    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) final;
    void Sync(IRetryableTransactionPtr transaction) final;

    IStateHolderPtr GetState(const TKey& key) final;

    TFuture<TListResult> List(TFilter filter, i64 limit, std::optional<TKey> offsetExclusive) final;

private:
    struct TEpochState
    {
        TStateSchemaPtr StateSchema;
        THashMap<TKey, TPayload> OldStates;
        THashMap<TKey, TStateHolderPtr> States;
    };

private:
    const TJobNamedStateCachePtr StateCache_;
    const NSimpleExternalState::TOperator Operator_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    std::optional<TEpochState> EpochState_;

    NSimpleExternalState::TCachedValuePtr ExtractCachedState(const TKey& key);
    void UpdateCache(const TKey& key, const TPayload& payload, const TStateSchemaPtr& schema);
};

DEFINE_REFCOUNTED_TYPE(TSimpleExternalStateManager);

////////////////////////////////////////////////////////////////////////////////

struct TSimpleExternalStateJoinerSpec
    : public IExternalStateJoiner::TParameters
{
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TSimpleExternalStateJoinerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSimpleExternalStateJoinerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSimpleExternalStateJoinerSpec
    : public IExternalStateJoiner::TDynamicParameters
{
    TDynamicExpiringJobNamedStateCacheSpecPtr Cache;

    REGISTER_YSON_STRUCT(TDynamicSimpleExternalStateJoinerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSimpleExternalStateJoinerSpec);

////////////////////////////////////////////////////////////////////////////////

//! Join-side counterpart of TSimpleExternalStateManager: loads per-key states
//! from YT on demand and caches them with a TTL.
class TSimpleExternalStateJoiner
    : public TExternalStateJoinerBase<TSimpleExternalState>
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TSimpleExternalStateJoinerSpec);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicSimpleExternalStateJoinerSpec);

    using TStateSchemaPtr = NTableClient::TTableSchemaPtr;

    TSimpleExternalStateJoiner(
        TExternalStateJoinerContextPtr context,
        TDynamicExternalStateJoinerContextPtr dynamicContext);

    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) final;
    void Reset() final;

    IStateHolderPtr GetState(const TKey& key) final;

    TFuture<TListResult> List(TFilter filter, i64 limit, std::optional<TKey> offsetExclusive) final;

private:
    using TCacheCookie = TExpiringJobNamedStateCache::TCookie;

    const TExpiringJobNamedStateCachePtr StateCache_;
    const NSimpleExternalState::TOperator Operator_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    TStateSchemaPtr StateSchema_;
    THashMap<TKey, TStateHolderPtr> States_;
    THashMap<TKey, TCacheCookie> Cookies_;

    struct TCachedExtraction
    {
        TPayload Payload;
        TStateSchemaPtr Schema;
        TCacheCookie Cookie;
    };

    std::optional<TCachedExtraction> ExtractCachedState(const TKey& key);
    void UpdateCache(
        const TKey& key,
        const TPayload& payload,
        const TStateSchemaPtr& schema,
        const std::optional<TCacheCookie>& cookie);
};

DEFINE_REFCOUNTED_TYPE(TSimpleExternalStateJoiner);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define SIMPLE_EXTERNAL_STATE_MANAGER_INL_H_
#include "simple_external_state_manager-inl.h"
#undef SIMPLE_EXTERNAL_STATE_MANAGER_INL_H_
