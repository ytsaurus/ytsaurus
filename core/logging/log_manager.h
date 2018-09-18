#pragma once

#include "public.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

struct TLogWritersCacheKey
{
    TString Category;
    ELogLevel LogLevel;
    ELogMessageFormat MessageFormat;
};

bool operator == (const TLogWritersCacheKey& lhs, const TLogWritersCacheKey& rhs);

////////////////////////////////////////////////////////////////////////////////

class TLogManager
    : public IShutdownable
{
public:
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
    TLogManager();

    Y_DECLARE_SINGLETON_FRIEND();

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

template <>
struct THash<NYT::NLogging::TLogWritersCacheKey>
{
    size_t operator () (const NYT::NLogging::TLogWritersCacheKey& obj) const
    {
        size_t hash = 0;
        NYT::HashCombine(hash, THash<TString>()(obj.Category));
        NYT::HashCombine(hash, static_cast<size_t>(obj.LogLevel));
        NYT::HashCombine(hash, static_cast<size_t>(obj.MessageFormat));
        return hash;
    }
};
