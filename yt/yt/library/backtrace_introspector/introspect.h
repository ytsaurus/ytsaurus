#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/tracing/public.h>

namespace NYT::NBacktraceIntrospector {

////////////////////////////////////////////////////////////////////////////////
// Thread introspection API

struct TThreadIntrospectionInfo
{
    NConcurrency::TThreadId ThreadId;
    NConcurrency::TFiberId FiberId;
    TString ThreadName;
    NTracing::TTraceId TraceId;
    //! Empty if no trace context is known.
    TString TraceLoggingTag;
    std::vector<const void*> Backtrace;
};

std::vector<TThreadIntrospectionInfo> IntrospectThreads();

////////////////////////////////////////////////////////////////////////////////
// Fiber introspection API

struct TFiberIntrospectionInfo
{
    NConcurrency::EFiberState State;
    NConcurrency::TFiberId FiberId;
    //! Zero if fiber is not waiting.
    TInstant WaitingSince;
    //! |InvalidThreadId| is fiber is not running.
    NConcurrency::TThreadId ThreadId;
    //! Empty if fiber is not running.
    TString ThreadName;
    NTracing::TTraceId TraceId;
    //! Empty if no trace context is known.
    TString TraceLoggingTag;
    std::vector<const void*> Backtrace;
};

std::vector<TFiberIntrospectionInfo> IntrospectFibers();

////////////////////////////////////////////////////////////////////////////////

TString FormatIntrospectionInfos(const std::vector<TThreadIntrospectionInfo>& infos);
TString FormatIntrospectionInfos(const std::vector<TFiberIntrospectionInfo>& infos);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktraceIntrospector
