#pragma once

#include "../fns.h"

#include <optional>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TFnAttributesOps
{
public:
    // Merge attributes of parDos. If one of IsPures is not true, then GetIsPure(result) is not true.
    static void Merge(TFnAttributes& destination, const TFnAttributes& source);

    static bool GetIsPure(const TFnAttributes& attributes);
    static void SetIsMove(TFnAttributes& attributes, bool isMove = true);
    static bool GetIsMove(const TFnAttributes& attributes);
    static const std::vector<TString> GetResourceFileList(const TFnAttributes& attributes);
    static void SetStateIds(TFnAttributes& attributes, decltype(TFnAttributes::StateIds_) stateIds);
    static decltype(TFnAttributes::StateIds_) GetStateIds(const TFnAttributes& attributes);
    static void SetTimerIds(TFnAttributes& attributes, decltype(TFnAttributes::TimerIds_) timerIds);
    static decltype(TFnAttributes::TimerIds_) GetTimerIds(const TFnAttributes& attributes);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
