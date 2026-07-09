#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBacktraceEnricherLevel,
    ((EnabledForAll)              (3))
    ((EnabledForTrivialErrors)    (2)) // Only for trivial errors (error codes: Generic or Canceled with message "Canceled").
    ((EnabledForNotNativeErrors)  (1)) // Only for non-NYT::TErrorException errors (they are also trivial).
    ((Disabled)                   (0))
);

////////////////////////////////////////////////////////////////////////////////

bool CheckLevel(const TError& error, EBacktraceEnricherLevel level);

////////////////////////////////////////////////////////////////////////////////

struct TBacktraceEnricherState
    : public TRefCounted
{
    std::atomic<EBacktraceEnricherLevel> Level;
    TConcurrentHashMap<std::string, std::string> CachedBacktraces;

    static const TBacktraceEnricherStatePtr& Get();
};

DEFINE_REFCOUNTED_TYPE(TBacktraceEnricherState)

////////////////////////////////////////////////////////////////////////////////

void EnrichErrorWithBacktrace(TError* error, const std::exception&);

void InitBacktraceEnricher();

////////////////////////////////////////////////////////////////////////////////

struct TBacktraceEnricherSpec
    : public NYTree::TYsonStruct
{
    EBacktraceEnricherLevel Level{};

    REGISTER_YSON_STRUCT(TBacktraceEnricherSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBacktraceEnricherSpec)

struct TBacktraceEnricherDynamicSpec
    : public NYTree::TYsonStruct
{
    std::optional<EBacktraceEnricherLevel> Level;

    REGISTER_YSON_STRUCT(TBacktraceEnricherDynamicSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBacktraceEnricherDynamicSpec)

void ConfigureSingleton(const TBacktraceEnricherSpecPtr& spec);

void ReconfigureSingleton(
    const TBacktraceEnricherSpecPtr& spec,
    const TBacktraceEnricherDynamicSpecPtr& dynamicSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
