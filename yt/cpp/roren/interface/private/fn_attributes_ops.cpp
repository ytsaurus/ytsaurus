#include "fn_attributes_ops.h"
#include <yt/cpp/roren/interface/fns.h>


namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void TFnAttributesOps::Merge(TFnAttributes& destination, const TFnAttributes& source)
{
    destination.IsPure_ = destination.IsPure_ && source.IsPure_;
    for (const auto& resourceFile : source.ResourceFileList_) {
        destination.ResourceFileList_.push_back(resourceFile);
    }
    destination.StateIds_.insert(source.StateIds_.begin(), source.StateIds_.end());
    destination.TimerIds_.insert(source.TimerIds_.begin(), source.TimerIds_.end());
}

bool TFnAttributesOps::GetIsPure(const TFnAttributes& attributes)
{
    return attributes.IsPure_;
}

void TFnAttributesOps::SetIsMove(TFnAttributes& attributes, bool isMove)
{
    attributes.IsMove_ = isMove;
}

bool TFnAttributesOps::GetIsMove(const TFnAttributes& attributes)
{
    return attributes.IsMove_;
}

const std::vector<TString> TFnAttributesOps::GetResourceFileList(const TFnAttributes& attributes)
{
    return attributes.ResourceFileList_;
}

void TFnAttributesOps::SetStateIds(TFnAttributes& attributes, decltype(TFnAttributes::StateIds_) stateIds)
{
    attributes.StateIds_ = std::move(stateIds);
}

decltype(TFnAttributes::StateIds_) TFnAttributesOps::GetStateIds(const TFnAttributes& attributes)
{
    return attributes.StateIds_;
}

void TFnAttributesOps::SetTimerIds(TFnAttributes& attributes, decltype(TFnAttributes::TimerIds_) timerIds)
{
    attributes.TimerIds_ = std::move(timerIds);
}

decltype(TFnAttributes::TimerIds_) TFnAttributesOps::GetTimerIds(const TFnAttributes& attributes)
{
    return attributes.TimerIds_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
