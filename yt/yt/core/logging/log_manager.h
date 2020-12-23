#pragma once

#include "public.h"

#include <yt/core/misc/shutdownable.h>
#include <yt/core/misc/singleton.h>

#include <yt/core/tracing/public.h>

#include <yt/core/ytree/public.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TLogManager
    : public IShutdownable
{
public:
    friend struct TLocalQueueReclaimer;

    ~TLogManager();

    static TLogManager* Get();

    static void StaticShutdown();

    void Configure(TLogManagerConfigPtr config);

    void ConfigureFromEnv();
    bool IsConfiguredFromEnv();

    virtual void Shutdown() override;

    const TLoggingCategory* GetCategory(TStringBuf categoryName);
    void UpdateCategory(TLoggingCategory* category);
    void UpdatePosition(TLoggingPosition* position, TStringBuf message);

    int GetVersion() const;
    bool GetAbortOnAlert() const;

    void Enqueue(TLogEvent&& event);

    void Reopen();
    void EnableReopenOnSighup();

    void SuppressRequest(NTracing::TRequestId requestId);

    void Synchronize(TInstant deadline = TInstant::Max());

private:
    TLogManager();

    DECLARE_LEAKY_SINGLETON_FRIEND();

    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

template <>
struct TSingletonTraits<NYT::NLogging::TLogManager>
{
    enum
    {
        Priority = 2048
    };
};
