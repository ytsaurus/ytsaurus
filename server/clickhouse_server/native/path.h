#pragma once

#include "public.h"

namespace NYT::NClickHouseServer::NNative {

////////////////////////////////////////////////////////////////////////////////

struct IPathService
{
    virtual ~IPathService() = default;

    virtual TString Join(
        const TString& base,
        const TString& relative) const = 0;

    virtual TString Build(
        const TString& base,
        std::vector<TString> relative) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

const IPathService* GetPathService();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
