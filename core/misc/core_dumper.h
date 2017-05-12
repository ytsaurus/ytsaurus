#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/nullable.h>

#include <util/system/mutex.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TCoreDump
{
    TString Path;
    TFuture<void> WrittenEvent;
};

////////////////////////////////////////////////////////////////////////////////

class TCoreDumper
    : public TRefCounted
{
public:
    explicit TCoreDumper(const TCoreDumperConfigPtr& config);

    TCoreDump WriteCoreDump(const std::vector<TString>& notes);

private:
    const TCoreDumperConfigPtr Config_;
    const NConcurrency::TActionQueuePtr ActionQueue_;
    TMutex Mutex_;
};

DEFINE_REFCOUNTED_TYPE(TCoreDumper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
