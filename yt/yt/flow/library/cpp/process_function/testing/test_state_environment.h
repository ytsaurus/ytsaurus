#pragma once

#include "entity_builders.h"
#include "in_memory_external_state_manager.h"

#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/computation/job_state/state_manager.h>

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <functional>
#include <memory>
#include <vector>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! Test fixture exposing an IRuntimeInitContext over fresh in-memory state tables, for
//! driving IProcessFunction::Init and reading back the state a function writes.
class TTestStateEnvironment
{
public:
    explicit TTestStateEnvironment(NTableClient::TTableSchemaPtr keySchema = DefaultTestKeySchema());

    const IRuntimeInitContextPtr& GetInitContext() const;
    const TJobStateManagerPtr& GetStateManager() const;

    //! Sets the static ``function_parameters`` node the init context hands to
    //! IRuntimeInitContext::GetParameters<T>(); rebuilds the init context. Call before Init.
    void SetStaticParametersNode(NYTree::IMapNodePtr node);

    //! Typed convenience over SetStaticParametersNode: serializes |parameters| to a node.
    template <class T>
    void SetStaticParameters(const TIntrusivePtr<T>& parameters)
    {
        SetStaticParametersNode(NYTree::ConvertTo<NYTree::IMapNodePtr>(parameters));
    }

    //! Persists pending state into the in-memory tables.
    void Sync();

    //! Preloads key states for the given input context, mirroring what the worker does
    //! before each Process. Must be called after the function's Init (which registers
    //! the state providers) and before reading state via GetState.
    void PreloadKeyStates(const IInputContextPtr& inputContext);

    //! Preloads internal and every registered external state for |input|'s keys.
    void PreloadEpoch(const IInputContextPtr& input);

    //! Commits internal state and every registered external store, ending an epoch. |enrollSync|,
    //! if given, is offered each store's transaction to enroll end-of-epoch writes.
    void CommitEpoch(const std::function<void(const IRetryableTransactionPtr&)>& enrollSync = {});

    //! Registers a commit run by CommitEpoch. The callback receives the epoch's transaction and
    //! commits it against the fixture's own client.
    void RegisterEpochCommit(std::function<void(const IRetryableTransactionPtr&)> commit);

    //! Registers an arbitrary external state manager under |name| (the same name the function
    //! passes to InitExternalStateClient). Lets a test plug in any IExternalStateManager — the
    //! in-memory simple one, or a real manager (e.g. a serializable-profile TProfileManager)
    //! constructed over a mock YT client.
    void RegisterExternalState(TStringBuf name, IExternalStateManagerPtr manager);

    //! Convenience over RegisterExternalState: builds an in-memory simple manager and returns
    //! it so the test can read the resulting state back via GetState(key) after the function ran.
    TInMemorySimpleExternalStateManagerPtr RegisterExternalState(
        TStringBuf name,
        NTableClient::TTableSchemaPtr stateSchema,
        NTableClient::TTableSchemaPtr keySchema = nullptr);

    //! Registers an arbitrary external state joiner under |name| (the name the function passes
    //! to InitExternalStateClient for a read-only TJoinedStateKeyClient).
    void RegisterExternalStateJoiner(TStringBuf name, IExternalStateJoinerPtr joiner);

    //! Convenience over RegisterExternalStateJoiner: builds an in-memory simple joiner and
    //! returns it so the test can seed per-key state via GetMutableState before the function runs.
    TInMemorySimpleExternalStateJoinerPtr RegisterExternalStateJoiner(
        TStringBuf name,
        NTableClient::TTableSchemaPtr stateSchema,
        NTableClient::TTableSchemaPtr keySchema = nullptr);

    //! Registers an internal state joiner under |name| reading this environment's own
    //! computation's internal state |stateName| (so a test can run the producing function and the
    //! joining function against one environment, then have the joiner read the produced state).
    //! |keySchemaOverride| mirrors the spec's join_on.key_schema_override (null = use the message
    //! key directly). `AutoPreload` is false, so the joining function must call `PreloadKeyStates`
    //! itself. Returns the spec so the test can also place it in the runtime context's spec
    //! (`SetSpec`) for functions that read `GetSpec()->StateJoiners`.
    TStateJoinerSpecPtr RegisterStateJoiner(
        TStringBuf name,
        TStringBuf stateName,
        NTableClient::TTableSchemaPtr keySchemaOverride = nullptr);

    //! Reads back the key state named |name| for |key| after the function ran. Persists
    //! current state (Sync) and reads it through a fresh manager over the same in-memory
    //! tables, so it both verifies persistence and avoids the one-provider-per-name limit
    //! of the live manager.
    template <class TState>
    TState ReadKeyState(TStringBuf name, const TKey& key)
    {
        auto initContext = MakeReloadedInitContext();
        auto client = NConcurrency::WaitFor(initContext->CreateMutableStateKeyClient<TState>(name)).ValueOrThrow();
        NConcurrency::WaitFor(client.PreloadKeyStates(THashSet<TKey>{key})).ThrowOnError();
        return *client.GetState(key);
    }

    //! Reads back external state of type |TState| for |key| from the manager registered under
    //! |name|, after the function ran. Symmetric to ReadKeyState (which reads internal state).
    template <class TState>
    TState ReadExternalKeyState(TStringBuf name, const TKey& key)
    {
        auto it = ExternalManagers_->find(ExtendStateNamePrefix(/*prefix*/ {}, name));
        YT_VERIFY(it != ExternalManagers_->end());
        auto holder = DynamicPointerCast<TStateHolder<TState>>(it->second->GetState(key));
        YT_VERIFY(holder);
        return holder->Get();
    }

private:
    using TExternalManagerMap = THashMap<std::string, IExternalStateManagerPtr>;
    using TExternalJoinerMap = THashMap<std::string, IExternalStateJoinerPtr>;

    TJobStateManagerContextPtr ManagerContext_;
    TJobStateManagerPtr StateManager_;
    std::shared_ptr<TExternalManagerMap> ExternalManagers_;
    std::shared_ptr<TExternalJoinerMap> ExternalJoiners_;
    IRuntimeInitContextPtr InitContext_;

    std::vector<std::function<void(const IRetryableTransactionPtr&)>> EpochCommits_;

    //! Syncs pending state and returns an init context over a fresh manager bound to the
    //! same in-memory tables.
    IRuntimeInitContextPtr MakeReloadedInitContext();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
