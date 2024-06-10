#include "stateful_par_do.h"

#include "../bigrt_execution_context.h"
#include "../stateful_timer_impl/stateful_timer_par_do.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TToStatefulTimerWrapper
    : public IRawStatefulTimerParDo
{
public:
    TToStatefulTimerWrapper(IRawStatefulParDoPtr underlying);

    void SetAttribute(const TString& key, const std::any& value) override;
    const std::any* GetAttribute(const TString& key) const override;

    TRowVtable GetStateVtable() const override;
    const TFnAttributes& GetFnAttributes() const override;
    void Start(const IExecutionContextPtr& context, IRawStateStorePtr rawStateMap, const std::vector<IRawOutputPtr>& outputs) override;
    void Do(const void* row, int count) override;
    void OnTimer(const void* rawKey, const TTimer& timer) override;
    void Finish() override;
    const TString& GetFnId() const override;

    [[nodiscard]] std::vector<TDynamicTypeTag> GetInputTags() const override;
    [[nodiscard]] std::vector<TDynamicTypeTag> GetOutputTags() const override;
    TDefaultFactoryFunc GetDefaultFactory() const override;
    void Save(IOutputStream* stream) const override;
    void Load(IInputStream* stream) override;


private:
    IRawStatefulParDoPtr Underlying_;
};

using TToStatefulTimerWrapperPtr = TIntrusivePtr<TToStatefulTimerWrapper>;

TToStatefulTimerWrapper::TToStatefulTimerWrapper(IRawStatefulParDoPtr underlying)
    : Underlying_(std::move(underlying))
{
}

void TToStatefulTimerWrapper::SetAttribute(const TString& key, const std::any& value)
{
    Underlying_->SetAttribute(key, value);
}

const std::any* TToStatefulTimerWrapper::GetAttribute(const TString& key) const
{
    return Underlying_->GetAttribute(key);
}

std::vector<TDynamicTypeTag> TToStatefulTimerWrapper::GetInputTags() const
{
    return Underlying_->GetInputTags();
}

std::vector<TDynamicTypeTag> TToStatefulTimerWrapper::GetOutputTags() const
{
    return Underlying_->GetOutputTags();
}

TToStatefulTimerWrapper::TDefaultFactoryFunc TToStatefulTimerWrapper::GetDefaultFactory() const
{
    Y_ABORT();
}

void TToStatefulTimerWrapper::Save(IOutputStream* stream) const
{
    Y_UNUSED(stream);
    Y_ABORT();
}

void TToStatefulTimerWrapper::Load(IInputStream* stream)
{
    Y_UNUSED(stream);
    Y_ABORT();
}

TRowVtable TToStatefulTimerWrapper::GetStateVtable() const
{
    return Underlying_->GetStateVtable();
}

const TFnAttributes& TToStatefulTimerWrapper::GetFnAttributes() const
{
    return Underlying_->GetFnAttributes();
}

void TToStatefulTimerWrapper::Start(const IExecutionContextPtr& context, IRawStateStorePtr rawStateMap, const std::vector<IRawOutputPtr>& outputs)
{
    Underlying_->Start(context, std::move(rawStateMap), outputs);
}

void TToStatefulTimerWrapper::Do(const void* row, int count)
{
    Underlying_->Do(row, count);
}

void TToStatefulTimerWrapper::OnTimer(const void* rawKey, const TTimer& timer)
{
    Y_UNUSED(rawKey, timer);
    Y_ABORT();
}

void TToStatefulTimerWrapper::Finish()
{
    Underlying_->Finish();
}

const TString& TToStatefulTimerWrapper::GetFnId() const
{
    Y_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

TToStatefulTimerWrapperPtr ToStatefulTimerWrapper(IRawStatefulParDoPtr underlying)
{
    return MakeIntrusive<TToStatefulTimerWrapper>(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

class TStatefulParDoWrapper
    : public TStatefulTimerParDoWrapper
{
public:
    TStatefulParDoWrapper(IRawStatefulParDoPtr underlying, TString stateManagerId, TBigRtStateManagerVtable stateManagerVtable, TBigRtStateConfig config)
        : TStatefulTimerParDoWrapper(ToStatefulTimerWrapper(std::move(underlying)), std::move(stateManagerId), std::move(stateManagerVtable), std::move(config))
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateStatefulParDo(
    IRawStatefulParDoPtr rawStatefulParDo,
    TString stateManagerId,
    TBigRtStateManagerVtable stateManagerVtable,
    TBigRtStateConfig stateConfig)
{
    return ::MakeIntrusive<TStatefulParDoWrapper>(
        std::move(rawStatefulParDo),
        std::move(stateManagerId),
        std::move(stateManagerVtable),
        std::move(stateConfig)
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
