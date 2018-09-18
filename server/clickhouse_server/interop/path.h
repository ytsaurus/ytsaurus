#pragma once

#include <util/generic/string.h>

#include <memory>
#include <vector>

namespace NInterop {

////////////////////////////////////////////////////////////////////////////////

class IPathService
{
public:
    virtual ~IPathService() = default;

    virtual TString Join(
        const TString& base,
        const TString& relative) const = 0;

    virtual TString Build(
        const TString& base,
        std::vector<TString> relative) const = 0;
};

}   // namespace NInterop
