#pragma once

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/state.h>

#include <yt/yt/client/table_client/public.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <optional>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TInMemorySimpleExternalStateManager)

//! In-memory IExternalStateManager over TSimpleExternalState for unit tests. Keeps per-key
//! state in a map (a fresh key starts as a null-filled row over the state schema); Sync is
//! a no-op since there is no backing table. Lets a test drive (and read back) functions
//! that use external state without a YT cluster.
class TInMemorySimpleExternalStateManager
    : public IExternalStateManager
{
public:
    explicit TInMemorySimpleExternalStateManager(
        NTableClient::TTableSchemaPtr stateSchema,
        NTableClient::TTableSchemaPtr keySchema = nullptr);

    IStateHolderPtr GetState(const TKey& key) override;
    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) override;
    NTableClient::TTableSchemaPtr GetKeySchema() const override;

    void Sync(IRetryableTransactionPtr transaction) override;
    void ValidateStateClass(const std::type_info& expectedStateType) const override;

    //! Entity-parameters plumbing required by IExternalStateManager; tests don't configure
    //! parameters, so these return freshly-defaulted parameter structs.
    TParametersPtr GetParametersBase() const override;
    TDynamicParametersPtr GetDynamicParametersBase() const override;

private:
    const NTableClient::TTableSchemaPtr StateSchema_;
    const NTableClient::TTableSchemaPtr KeySchema_;
    THashMap<TKey, TIntrusivePtr<TStateHolder<TSimpleExternalState>>> States_;
};

DEFINE_REFCOUNTED_TYPE(TInMemorySimpleExternalStateManager)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TInMemorySimpleExternalStateJoiner)

//! In-memory IExternalStateJoiner over TSimpleExternalState for unit tests; the read-only
//! counterpart of TInMemorySimpleExternalStateManager. A test seeds per-key state via
//! GetMutableState(), then the function reads it through a TJoinedStateKeyClient. Declares no
//! key-provider streams (matches any input stream) and no key-schema override, so a key is
//! taken verbatim from the message/timer. Reset and PreloadKeyStates are no-ops (the seeded
//! map is the whole store).
class TInMemorySimpleExternalStateJoiner
    : public IExternalStateJoiner
{
public:
    explicit TInMemorySimpleExternalStateJoiner(
        NTableClient::TTableSchemaPtr stateSchema,
        NTableClient::TTableSchemaPtr keySchema = nullptr);

    //! Returns the mutable typed holder for |key| (lazily created as a null-filled row over
    //! the state schema), so a test can seed the read-only state before the function runs.
    TIntrusivePtr<TStateHolder<TSimpleExternalState>> GetMutableState(const TKey& key);

    IStateHolderPtr GetState(const TKey& key) override;
    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) override;
    NTableClient::TTableSchemaPtr GetKeySchema() const override;
    const IPayloadConverterCachePtr& GetConverterCache() const override;
    const std::optional<THashSet<TStreamId>>& GetKeyProviderStreams() const override;
    bool HasKeySchemaOverride() const override;

    void Reset() override;
    void ValidateStateClass(const std::type_info& expectedStateType) const override;

    //! Entity-parameters plumbing required by IExternalStateJoiner; tests don't configure
    //! parameters, so these return freshly-defaulted parameter structs.
    TParametersPtr GetParametersBase() const override;
    TDynamicParametersPtr GetDynamicParametersBase() const override;

private:
    const NTableClient::TTableSchemaPtr StateSchema_;
    const NTableClient::TTableSchemaPtr KeySchema_;
    //! Null; only consulted under HasKeySchemaOverride(), which is false here.
    const IPayloadConverterCachePtr ConverterCache_;
    //! nullopt — the joiner accepts a key from any input stream.
    const std::optional<THashSet<TStreamId>> KeyProviderStreams_;
    THashMap<TKey, TIntrusivePtr<TStateHolder<TSimpleExternalState>>> States_;
};

DEFINE_REFCOUNTED_TYPE(TInMemorySimpleExternalStateJoiner)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
