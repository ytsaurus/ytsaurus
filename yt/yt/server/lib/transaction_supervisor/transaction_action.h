#pragma once

#include "public.h"

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/core/phoenix/context.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

template <class TSaveContext, class TLoadContext>
struct ITransactionActionState
    : public NPhoenix::ICustomPersistent<TSaveContext, TLoadContext>
{
    virtual void* GetUnderlying() = 0;
};

template <class TTransaction, class TProto, class TState>
struct TTypedTransactionActionDescriptor
{
    template <class TReturnType, class... TArgs>
    using THandler = std::conditional_t<
        std::is_same_v<TState, void>,
        TCallback<TReturnType(TTransaction*, TProto*, TArgs...)>,
        TCallback<TReturnType(TTransaction*, TProto*, TState*, TArgs...)>
    >;

    template <class... TArgs>
    using TVoidHandler = THandler<void, TArgs...>;

    THandler<THashSet<std::string>> GenerateStrongOrderingTags;
    TVoidHandler<const TTransactionPrepareOptions&> Prepare;
    TVoidHandler<const TTransactionCommitOptions&> Commit;
    TVoidHandler<const TTransactionAbortOptions&> Abort;
    TVoidHandler<> Serialize;

    static std::string GetType();
};

template <class TTransaction, class TSaveContext, class TLoadContext>
class TTypeErasedTransactionActionDescriptor
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::string, Type);

public:
    template <class TProto, class TState>
    explicit TTypeErasedTransactionActionDescriptor(
        TTypedTransactionActionDescriptor<TTransaction, TProto, TState> descriptor);

    using IActionState = ITransactionActionState<TSaveContext, TLoadContext>;
    std::unique_ptr<IActionState> CreateState() const;

    THashSet<std::string> GenerateStrongOrderingTags(TTransaction* transaction, TStringBuf value, IActionState* state) const;
    void Prepare(TTransaction* transaction, TStringBuf value, IActionState* state, const TTransactionPrepareOptions& options) const;
    void Commit(TTransaction* transaction, TStringBuf value, IActionState* state, const TTransactionCommitOptions& options) const;
    void Abort(TTransaction* transaction, TStringBuf value, IActionState* state, const TTransactionAbortOptions& options) const;
    void Serialize(TTransaction* transaction, TStringBuf value, IActionState* state) const;

    bool HasSerializeHandler() const;

private:
    const std::function<std::unique_ptr<IActionState>()> CreateState_;

    template <class TReturnType, class... TArgs>
    using THandler = TCallback<TReturnType(
        TTransaction* transaction,
        TStringBuf value,
        IActionState* state,
        TArgs...)
    >;

    template <class... TArgs>
    using TVoidHandler = THandler<void, TArgs...>;

    const THandler<THashSet<std::string>> GenerateStrongOrderingTags_;
    const TVoidHandler<const TTransactionPrepareOptions&> Prepare_;
    const TVoidHandler<const TTransactionCommitOptions&> Commit_;
    const TVoidHandler<const TTransactionAbortOptions&> Abort_;
    const TVoidHandler<> Serialize_;
};

template <class TSaveContext, class TLoadContext>
struct ITransactionActionStateFactory
{
    virtual ~ITransactionActionStateFactory() = default;

    using IActionState = ITransactionActionState<TSaveContext, TLoadContext>;
    virtual std::unique_ptr<IActionState> CreateTransactionActionState(TStringBuf type) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor

#define TRANSACTION_ACTION_INL_H_
#include "transaction_action-inl.h"
#undef TRANSACTION_ACTION_INL_H_
