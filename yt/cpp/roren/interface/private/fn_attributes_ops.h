#pragma once

#include "../fns.h"

#include <optional>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TFnAttributesOps
{
public:
    static void Merge(TFnAttributes& destination, const TFnAttributes& source);

    static bool GetIsPure(const TFnAttributes& attributes);
    static const std::vector<TString> GetResourceFileList(const TFnAttributes& attributes);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
