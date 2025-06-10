#pragma once

#include "transaction_manager.h"
#include "transaction_action.h"

#include <yt/yt/ytlib/transaction_client/action.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

template <class TBase, class TSaveContext, class TLoadContext>
class TTransactionBase
    : public TBase
{
public:
    using IActionState = ITransactionActionState<TSaveContext, TLoadContext>;
    using IActionStateFactory = ITransactionActionStateFactory<TSaveContext, TLoadContext>;

    struct TAction
        : public TTransactionActionData
    {
        TAction() = default;
        explicit TAction(NTransactionClient::TTransactionActionData&& data);

        std::unique_ptr<IActionState> State;
    };

    DEFINE_BYREF_RW_PROPERTY(std::vector<TAction>, Actions);

    // If prepare phase was finished before update to current version this field
    // is |nullopt|.
    // COMPAT(kvk1920): Make it non-optional after 25.2.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<int>, PreparedActionCount);

public:
    using TBase::TBase;

    virtual ~TTransactionBase() = default;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    ETransactionState GetPersistentState() const;
    void SetPersistentState(ETransactionState state);

    ETransactionState GetTransientState() const;
    void SetTransientState(ETransactionState state);

    ETransactionState GetState(bool persistent) const;

    void ResetTransientState();

    void ThrowInvalidState() const;

protected:
    virtual IActionStateFactory* GetActionStateFactory();

private:
    ETransactionState State_ = ETransactionState::Active;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor

#define TRANSACTION_DETAIL_INL_H_
#include "transaction_detail-inl.h"
#undef TRANSACTION_DETAIL_INL_H_
