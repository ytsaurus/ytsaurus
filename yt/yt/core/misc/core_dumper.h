#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/ytree/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TCoreDump
{
    TString Path;
    TFuture<void> WrittenEvent;
};

////////////////////////////////////////////////////////////////////////////////

struct ICoreDumper
    : public virtual TRefCounted
{
    virtual TCoreDump WriteCoreDump(const std::vector<TString>& notes, const TString& reason) = 0;

    virtual const NYTree::IYPathServicePtr& CreateOrchidService() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ICoreDumper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
