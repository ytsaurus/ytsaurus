#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/fwd.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct IQueryCorpusReporter
    : public TRefCounted
{
    virtual void AddQuery(TStringBuf query) = 0;
    virtual void Reconfigure(const TQueryCorpusReporterConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueryCorpusReporter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
