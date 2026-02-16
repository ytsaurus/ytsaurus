#pragma once

#include "folding_profiler.h"
#include "public.h"

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <llvm/ADT/FoldingSet.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TCodegenCacheConfig
    : public TSlruCacheConfig
{
    REGISTER_YSON_STRUCT(TCodegenCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCodegenCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCodegenCacheDynamicConfig
    : public TSlruCacheDynamicConfig
{
    REGISTER_YSON_STRUCT(TCodegenCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCodegenCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCachedCGQueryImage
    : public TAsyncCacheValueBase<llvm::FoldingSetNodeID, TCachedCGQueryImage>
{
    const std::string Fingerprint;
    const TCGQueryImage Image;

    TCachedCGQueryImage(const llvm::FoldingSetNodeID& id, std::string fingerprint, TCGQueryImage image);
};

using TCachedCGQueryImagePtr = TIntrusivePtr<TCachedCGQueryImage>;

////////////////////////////////////////////////////////////////////////////////

class TCodegenCacheSingleton
{
public:
    static TCachedCGQueryImagePtr Compile(
        bool enableCodeCache,
        const llvm::FoldingSetNodeID& id,
        const std::string& fingerprint,
        const TCGQueryGenerator& compileCallback,
        TDuration* const codegenTime,
        const NLogging::TLogger& Logger);

    static void Reconfigure(const TCodegenCacheDynamicConfigPtr& config);

    //! Only for testing purposes.
    static void TearDownForTests();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
