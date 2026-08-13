#pragma once

#include "public.h"

#include "codec.h"

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_provider.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCompanionInternalStateProvider);
DECLARE_REFCOUNTED_CLASS(TCompanionExternalStateManager);
DECLARE_REFCOUNTED_CLASS(TCompanionExternalStateJoiner);
DECLARE_REFCOUNTED_CLASS(TCompanionStateStore);

struct TCompanionExternalStateJoinerConfig
{
    NTableClient::TTableSchemaPtr KeySchema;
    IPayloadConverterCachePtr ConverterCache;
    std::optional<THashSet<TStreamId>> KeyProviderStreams;
    bool HasKeySchemaOverride = false;
};

////////////////////////////////////////////////////////////////////////////////

//! Internal-state backend over the per-batch wire content. Holders are created by the
//! typed client's ctor and deserialized from the incoming YSON bytes; modified states
//! are detected by serialize-and-compare against the incoming bytes.
class TCompanionInternalStateProvider
    : public IMutableStateKeyProvider
{
public:
    TCompanionInternalStateProvider(
        std::string name,
        std::function<IStateHolderPtr()> ctor,
        NTableClient::TTableSchemaPtr keySchema);

    IStateHolderPtr GetState(const TKey& key) override;
    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) override;
    NTableClient::TTableSchemaPtr GetKeySchema() const override;

    //! |incoming| may be null: the batch carries no items of this state.
    void LoadBatch(const NCompanion::TStateHolder<std::string>* incoming);
    void CollectModified(
        std::vector<NCompanion::TStateItem<std::string>>* items,
        const THashSet<TKey>& batchKeys) const;

private:
    const std::string Name_;
    const std::function<IStateHolderPtr()> Ctor_;
    const NTableClient::TTableSchemaPtr KeySchema_;

    THashMap<TKey, IStateHolderPtr> Holders_;
    THashMap<TKey, std::string> Incoming_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionInternalStateProvider);

////////////////////////////////////////////////////////////////////////////////

//! External-state backend over the per-batch wire content (rows + schema). The state
//! type is #TSimpleExternalState, matching the worker-side simple external manager.
class TCompanionExternalStateManager
    : public IExternalStateManager
{
public:
    TCompanionExternalStateManager(
        std::string name,
        NTableClient::TTableSchemaPtr keySchema);

    IStateHolderPtr GetState(const TKey& key) override;
    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) override;
    NTableClient::TTableSchemaPtr GetKeySchema() const override;

    void Sync(IRetryableTransactionPtr transaction) override;
    void ValidateStateClass(const std::type_info& expectedStateType) const override;

    TParametersPtr GetParametersBase() const override;
    TDynamicParametersPtr GetDynamicParametersBase() const override;

    void LoadBatch(const NCompanion::TStateHolder<TPayload>* incoming);
    void CollectModified(
        NCompanion::TStateHolder<TPayload>* holder,
        const THashSet<TKey>& batchKeys) const;

private:
    const std::string Name_;
    const NTableClient::TTableSchemaPtr KeySchema_;

    //! Learned from the batch content; external state rows always travel with a schema.
    NTableClient::TTableSchemaPtr StateSchema_;
    THashMap<TKey, TIntrusivePtr<TStateHolder<TSimpleExternalState>>> Holders_;
    THashMap<TKey, TPayload> Incoming_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionExternalStateManager);

////////////////////////////////////////////////////////////////////////////////

//! Read-only joined-external-state backend over the per-batch wire content.
//! A key the batch carried no state for surfaces as an uninitialized accessor.
//! Under a key schema override #GetKeySchema() drops the expression columns; a state is addressable
//! by that key and by the full key the worker sent.
class TCompanionExternalStateJoiner
    : public IExternalStateJoiner
{
public:
    TCompanionExternalStateJoiner(
        std::string name,
        TCompanionExternalStateJoinerConfig config);

    IStateHolderPtr GetState(const TKey& key) override;
    TFuture<void> PreloadKeyStates(const THashSet<TKey>& keys) override;
    NTableClient::TTableSchemaPtr GetKeySchema() const override;
    const IPayloadConverterCachePtr& GetConverterCache() const override;
    const std::optional<THashSet<TStreamId>>& GetKeyProviderStreams() const override;
    bool HasKeySchemaOverride() const override;

    void Reset() override;
    void ValidateStateClass(const std::type_info& expectedStateType) const override;

    TParametersPtr GetParametersBase() const override;
    TDynamicParametersPtr GetDynamicParametersBase() const override;

    void LoadBatch(const NCompanion::TStateHolder<TPayload>* incoming);

private:
    const std::string Name_;
    //! The key schema as the worker sends it, expression columns included.
    const NTableClient::TTableSchemaPtr WireKeySchema_;
    //! Under a key schema override, #WireKeySchema_ without its expression columns.
    const NTableClient::TTableSchemaPtr KeySchema_;
    const IPayloadConverterCachePtr ConverterCache_;
    const std::optional<THashSet<TStreamId>> KeyProviderStreams_;
    const bool HasKeySchemaOverride_;

    THashMap<TKey, TIntrusivePtr<TStateHolder<TSimpleExternalState>>> Holders_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionExternalStateJoiner);

////////////////////////////////////////////////////////////////////////////////

//! Per-job state facade: the state backends the function's clients are bound to at Init
//! time, reloaded from the wire before every batch. Not thread-safe; the caller
//! serializes batches per job.
class TCompanionStateStore
    : public TRefCounted
{
public:
    TCompanionStateStore(
        THashSet<std::string> internalStateNames,
        THashSet<std::string> externalStateNames,
        THashSet<std::string> joinedStateNames,
        NTableClient::TTableSchemaPtr keySchema,
        THashMap<std::string, TCompanionExternalStateJoinerConfig> joinedStateConfigs = {});

    //! Called during function Init; |name| must be declared in the computation's
    //! |internal_states| parameter.
    IMutableStateKeyProviderPtr RegisterInternalState(
        const std::string& name,
        std::function<IStateHolderPtr()> ctor);
    //! Called during function Init; |name| must be a declared external state manager.
    IExternalStateManagerPtr GetExternalStateManager(const std::string& name);
    //! Called during function Init; |name| must be a declared external state joiner.
    IExternalStateJoinerPtr GetExternalStateJoiner(const std::string& name);

    //! Drops per-key state of the previous batch and loads the new batch content.
    void LoadBatch(const TBatchInput& input);

    //! Returns only the states changed by the current batch (the incremental-state
    //! contract). Throws if a state was written for a key outside the batch.
    void CollectModified(
        std::vector<NCompanion::TStateHolder<std::string>>* internalStates,
        std::vector<NCompanion::TStateHolder<TPayload>>* externalStates) const;

private:
    //! Canonical (slash-prefixed) name -> the verbatim declared name, which is
    //! what travels on the wire.
    const THashMap<std::string, std::string> InternalStateNames_;
    const THashMap<std::string, std::string> ExternalStateNames_;
    const THashMap<std::string, std::string> JoinedStateNames_;
    const NTableClient::TTableSchemaPtr KeySchema_;
    const THashMap<std::string, TCompanionExternalStateJoinerConfig> JoinedStateConfigs_;

    THashMap<std::string, TCompanionInternalStateProviderPtr> InternalStates_;
    THashMap<std::string, TCompanionExternalStateManagerPtr> ExternalStates_;
    THashMap<std::string, TCompanionExternalStateJoinerPtr> JoinedStates_;

    THashSet<TKey> BatchKeys_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionStateStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
