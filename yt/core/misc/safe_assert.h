#pragma once

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/common.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Thrown when an assertion is not satisfied and SafeAssertionMode = true.
class TAssertionFailedException
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TString, Expression);
    DEFINE_BYVAL_RO_PROPERTY(TString, StackTrace);
    DEFINE_BYVAL_RO_PROPERTY(TNullable<TString>, CorePath);

public:
    TAssertionFailedException(
        const TString& expression,
        const TString& stackTrace,
        const TNullable<TString>& corePath);
};

////////////////////////////////////////////////////////////////////////////////

class TSafeAssertionsGuard
{
public:
    TSafeAssertionsGuard() = default;
    TSafeAssertionsGuard(
        TCoreDumperPtr coreDumper,
        NConcurrency::TAsyncSemaphorePtr coreSemaphore,
        std::vector<TString> coreNotes = {});
    ~TSafeAssertionsGuard();

    TSafeAssertionsGuard(const TSafeAssertionsGuard& other) = delete;
    TSafeAssertionsGuard(TSafeAssertionsGuard&& other);

    TSafeAssertionsGuard& operator=(const TSafeAssertionsGuard& other) = delete;
    TSafeAssertionsGuard& operator=(TSafeAssertionsGuard&& other);

private:
    bool Active_ = false;

    void Release();
};

////////////////////////////////////////////////////////////////////////////////

void PushSafeAssertionsFrame(
    TCoreDumperPtr coreDumper,
    NConcurrency::TAsyncSemaphorePtr coreSemaphore,
    std::vector<TString> coreNotes);

bool SafeAssertionsModeEnabled();

TCoreDumperPtr GetSafeAssertionsCoreDumper();

NConcurrency::TAsyncSemaphorePtr GetSafeAssertionsCoreSemaphore();

std::vector<TString> GetSafeAssertionsCoreNotes();

void PopSafeAssertionsFrame();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
