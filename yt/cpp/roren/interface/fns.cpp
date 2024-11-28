#include "fns.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TFnAttributes& TFnAttributes::SetIsPure(bool isPure)
{
    IsPure_ = isPure;
    return *this;
}

TFnAttributes& TFnAttributes::AddResourceFile(const TString& resourceFile) &
{
    ResourceFileList_.push_back(resourceFile);
    return *this;
}

TFnAttributes TFnAttributes::AddResourceFile(const TString& resourceFile) &&
{
    ResourceFileList_.push_back(resourceFile);
    return std::move(*this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
