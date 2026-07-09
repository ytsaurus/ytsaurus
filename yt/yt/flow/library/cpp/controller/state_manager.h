#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/state.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

class TStateManager
    : public TRefCounted
{
public:
    explicit TStateManager(TJobManagerStatePtr remoteState);

    IInitContextPtr CreateContext(const TComputationId& computationId, std::string prefix = "");

    void Sync();

    IStateHolderPtr CreateState(const TComputationId& computationId, const std::string& name, std::function<IStateHolderPtr()> ctor);

private:
    const TJobManagerStatePtr RemoteState_;
    THashMap<TComputationId, THashMap<std::string, TWeakPtr<IStateHolder>>> States_;
};

using TStateManagerPtr = TIntrusivePtr<TStateManager>;

////////////////////////////////////////////////////////////////////////////////

class TMutableStateProvider
    : public IMutableStateProvider
{
public:
    explicit TMutableStateProvider(IStateHolderPtr state);

    IStateHolderPtr GetState() override;

private:
    const IStateHolderPtr State_;
};

using TMutableStateProviderPtr = TIntrusivePtr<TMutableStateProvider>;

////////////////////////////////////////////////////////////////////////////////

class TInitContext
    : public IInitContext
{
public:
    explicit TInitContext(
        TStateManagerPtr stateManager,
        TComputationId computationId,
        std::string prefix);

    IInitContextPtr WithPrefix(TStringBuf prefix) const override;
    const std::string& GetPrefix() const override;

    TFuture<IMutableStateProviderPtr> CreateMutableStateProvider(std::function<IStateHolderPtr()> ctor) const override;

private:
    const TStateManagerPtr StateManager_;
    const TComputationId ComputationId_;
    const std::string Prefix_;
};

using TInitContextPtr = TIntrusivePtr<TInitContext>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
