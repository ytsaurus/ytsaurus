#include "cg_cache.h"

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TCodegenCacheConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* config) {
        config->Capacity = 512;
        config->ShardCount = 1;
    });
}

void TCodegenCacheDynamicConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

class TCodegenCacheSingletonImpl
    : public TAsyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedCGQueryImage>
{
public:
    TCodegenCacheSingletonImpl()
        : TAsyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedCGQueryImage>(
            New<TCodegenCacheConfig>(),
            TProfiler(std::string(ProfilerName)))
    { }

    static TCodegenCacheSingletonImpl* GetInstance()
    {
        return LeakyRefCountedSingleton<TCodegenCacheSingletonImpl>().Get();
    }

    //! Only for testing purposes.
    static void TearDownForTests()
    {
        auto instance = GetInstance();
        auto values = instance->GetAll();
        for (auto& value : values) {
            instance->TryRemoveValue(value);
        }
    }

    TCachedCGQueryImagePtr CompileWithCache(
        const llvm::FoldingSetNodeID& id,
        const std::string& fingerprint,
        const TCGQueryGenerator& compileCallback,
        TDuration* const codegenTime,
        const NLogging::TLogger& Logger)
    {
        auto cookie = BeginInsert(id);
        if (cookie.IsActive()) {
            YT_LOG_DEBUG("Codegen cache miss: generating query evaluator");

            try {
                cookie.EndInsert(CompileWithLogging(id, fingerprint, compileCallback, codegenTime, Logger));
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Failed to compile a query fragment");
                cookie.Cancel(TError(ex).Wrap("Failed to compile a query fragment"));
            }
        }

        return WaitForFast(cookie.GetValue())
            .ValueOrThrow();
    }

    TCachedCGQueryImagePtr CompileWithoutCache(
        const llvm::FoldingSetNodeID& id,
        const std::string& fingerprint,
        const TCGQueryGenerator& compileCallback,
        TDuration* const codegenTime,
        const NLogging::TLogger& Logger)
    {
        YT_LOG_DEBUG("Codegen cache disabled");
        return CompileWithLogging(id, fingerprint, compileCallback, codegenTime, Logger);
    }

private:
    static constexpr TStringBuf ProfilerName = "/codegen_cache";

    static TCachedCGQueryImagePtr CompileWithLogging(
        const llvm::FoldingSetNodeID& id,
        const std::string& fingerprint,
        const TCGQueryGenerator& compileCallback,
        TDuration* const codegenTime,
        const NLogging::TLogger& Logger)
    {
        auto traceContextGuard = NTracing::TChildTraceContextGuard("QueryClient.Compile");
        YT_LOG_DEBUG("Started compiling fragment");
        auto timingGuard = TValueIncrementingTimingGuard<TFiberWallTimer>(codegenTime);
        auto image = compileCallback();
        auto cachedImage = New<TCachedCGQueryImage>(id, fingerprint, std::move(image));
        YT_LOG_DEBUG("Finished compiling fragment");
        return cachedImage;
    }
};

////////////////////////////////////////////////////////////////////////////////

TCachedCGQueryImage::TCachedCGQueryImage(const llvm::FoldingSetNodeID& id, std::string fingerprint, TCGQueryImage image)
    : TAsyncCacheValueBase(id)
    , Fingerprint(std::move(fingerprint))
    , Image(std::move(image))
{ }

////////////////////////////////////////////////////////////////////////////////

TCachedCGQueryImagePtr TCodegenCacheSingleton::Compile(
    bool enableCodeCache,
    const llvm::FoldingSetNodeID& id,
    const std::string& fingerprint,
    const TCGQueryGenerator& compileCallback,
    TDuration* const codegenTime,
    const NLogging::TLogger& Logger)
{
    auto instance = TCodegenCacheSingletonImpl::GetInstance();

    return enableCodeCache
        ? instance->CompileWithCache(id, fingerprint, compileCallback, codegenTime, Logger)
        : instance->CompileWithoutCache(id, fingerprint, compileCallback, codegenTime, Logger);
}

void TCodegenCacheSingleton::Reconfigure(const TCodegenCacheDynamicConfigPtr& config)
{
    TCodegenCacheSingletonImpl::GetInstance()->Reconfigure(config);
}

void TCodegenCacheSingleton::TearDownForTests()
{
    TCodegenCacheSingletonImpl::TearDownForTests();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
