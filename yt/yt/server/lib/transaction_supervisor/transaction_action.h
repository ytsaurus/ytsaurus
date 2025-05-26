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
    template <class... TArgs>
    using THandler = std::conditional_t<
        std::is_same_v<TState, void>,
        TCallback<void(TTransaction*, TProto*, TArgs...)>,
        TCallback<void(TTransaction*, TProto*, TState*, TArgs...)>
    >;

    THandler<const TTransactionPrepareOptions&> Prepare;
    THandler<const TTransactionCommitOptions&> Commit;
    THandler<const TTransactionAbortOptions&> Abort;
    THandler<> Serialize;

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

    void Prepare(TTransaction* transaction, TStringBuf value, IActionState* state, const TTransactionPrepareOptions& options) const;
    void Commit(TTransaction* transaction, TStringBuf value, IActionState* state, const TTransactionCommitOptions& options) const;
    void Abort(TTransaction* transaction, TStringBuf value, IActionState* state, const TTransactionAbortOptions& options) const;
    void Serialize(TTransaction* transaction, TStringBuf value, IActionState* state) const;

    bool HasSerializeHandler() const;

private:
    const std::function<std::unique_ptr<IActionState>()> CreateState_;

    template <class... TArgs>
    using THandler = TCallback<void(
        TTransaction* transaction,
        TStringBuf value,
        IActionState* state,
        TArgs...)
    >;
    const THandler<const TTransactionPrepareOptions&> Prepare_;
    const THandler<const TTransactionCommitOptions&> Commit_;
    const THandler<const TTransactionAbortOptions&> Abort_;
    const THandler<> Serialize_;
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
