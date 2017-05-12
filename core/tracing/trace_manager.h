#pragma once

#include "public.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/core/ytree/public.h>

#include <util/generic/singleton.h>

namespace NYT {
namespace NTracing {

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

    void Enqueue(
        const NTracing::TTraceContext& context,
        const TString& serviceName,
        const TString& spanName,
        const TString& annotationName);

    void Enqueue(
        const NTracing::TTraceContext& context,
        const TString& annotationKey,
        const TString& annotationValue);

private:
    TTraceManager();

    Y_DECLARE_SINGLETON_FRIEND();

    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT

