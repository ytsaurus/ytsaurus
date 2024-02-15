#include "fns.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TFnAttributes& TFnAttributes::SetIsPure(bool isPure)
{
    IsPure_ = isPure;
    return *this;
}

TFnAttributes& TFnAttributes::SetName(std::optional<TString> name) &
{
    Name_ = std::move(name);
    return *this;
}

TFnAttributes TFnAttributes::SetName(std::optional<TString> name) &&
{
    Name_ = std::move(name);
    return std::move(*this);
}

const std::optional<TString>& TFnAttributes::GetName() const
{
    return Name_;
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
