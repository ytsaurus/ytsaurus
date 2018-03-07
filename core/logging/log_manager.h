#pragma once

#include "public.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

class TLogManager
    : public IShutdownable
{
public:
    TLogManager();
    ~TLogManager();

    static TLogManager* Get();

    static void StaticShutdown();

    void Configure(TLogConfigPtr config);
    void ConfigureFromEnv();

    virtual void Shutdown() override;

    const TLoggingCategory* GetCategory(const char* categoryName);
    void UpdateCategory(TLoggingCategory* category);
    void UpdatePosition(TLoggingPosition* position, const TString& message);

    int GetVersion() const;

    void Enqueue(TLogEvent&& event);

    void Reopen();

    void SetPerThreadBatchingPeriod(TDuration value);
    TDuration GetPerThreadBatchingPeriod() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT

template <>
struct TSingletonTraits<NYT::NLogging::TLogManager>
{
    enum
    {
        Priority = 2048
    };
};
