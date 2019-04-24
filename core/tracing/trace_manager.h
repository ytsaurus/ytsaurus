#pragma once

#include "public.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/core/misc/ref.h>

#include <yt/core/ytree/public.h>

#include <util/generic/singleton.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTraceManager
    : public IShutdownable
{
public:
    ~TTraceManager();

    static TTraceManager* Get();

    static void StaticShutdown();

    void Configure(TTraceManagerConfigPtr config);

    virtual void Shutdown() override;

    void Enqueue(NTracing::TTraceContextPtr traceContext);

    std::pair<i64, std::vector<TSharedRef>> ReadTraces(i64 startIndex, i64 limit);

private:
    TTraceManager();

    Y_DECLARE_SINGLETON_FRIEND();

    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

