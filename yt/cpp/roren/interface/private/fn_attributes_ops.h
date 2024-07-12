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
    // Apply patch to parDo attributes. If patch.IsPure_ is not set, GetIsPure(destination) is not changed.
    // Otherwise GetIsPure(destination) becomes equal to GetIsPure(patch).
    static void MergePatch(TFnAttributes& destination, const TFnAttributes& patch);

    static bool GetIsPure(const TFnAttributes& attributes);
    static const std::vector<TString> GetResourceFileList(const TFnAttributes& attributes);
private:
    static void Merge(TFnAttributes& destination, const TFnAttributes& source, bool isPatch);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
