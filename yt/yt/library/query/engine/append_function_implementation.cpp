#include "functions_cg.h"

#include <yt/yt/library/query/engine_api/append_function_implementation.h>
#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/library/query/base/private.h>

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NQueryClient {

constinit const auto Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TUdfObjectCodeKey
{
    std::string Fingerprint;
    std::string FunctionName;
    std::string SymbolName;

    operator size_t() const
    {
        size_t seed = 0;
        HashCombine(seed, Fingerprint);
        HashCombine(seed, FunctionName);
        HashCombine(seed, SymbolName);
        return seed;
    }

    bool operator==(const TUdfObjectCodeKey& other) const
    {
        return Fingerprint == other.Fingerprint
            && FunctionName == other.FunctionName
            && SymbolName == other.SymbolName;
    }
};

class TUdfObjectCodeCacheEntry
    : public TAsyncCacheValueBase<TUdfObjectCodeKey, TUdfObjectCodeCacheEntry>
{
public:
    TUdfObjectCodeCacheEntry(const TUdfObjectCodeKey& key, NCodegen::IObjectCodePtr objectCode)
        : TAsyncCacheValueBase(key)
        , ObjectCode_(std::move(objectCode))
    { }

    const NCodegen::IObjectCodePtr& GetObjectCode() const
    {
        return ObjectCode_;
    }

private:
    NCodegen::IObjectCodePtr ObjectCode_;
};

using TUdfObjectCodeCacheEntryPtr = TIntrusivePtr<TUdfObjectCodeCacheEntry>;

class TUdfObjectCodeCache
    : public TAsyncSlruCacheBase<TUdfObjectCodeKey, TUdfObjectCodeCacheEntry>
{
public:
    using TAsyncSlruCacheBase::TAsyncSlruCacheBase;

    NCodegen::IObjectCodePtr FindOrInsert(
        const TUdfObjectCodeKey& key,
        std::function<NCodegen::IObjectCodePtr()> makeObjectCode)
    {
        auto cookie = BeginInsert(key);
        if (!cookie.IsActive()) {
            return NConcurrency::WaitFor(cookie.GetValue())
                .ValueOrThrow()
                ->GetObjectCode();
        }

        try {
            auto objectCode = makeObjectCode();
            cookie.EndInsert(New<TUdfObjectCodeCacheEntry>(key, objectCode));
            return objectCode;
        } catch (const std::exception& ex) {
            cookie.Cancel(TError(ex));
            throw;
        }
    }
};

TUdfObjectCodeCache* GetUdfObjectCodeCache()
{
    struct THolder
    {
        TIntrusivePtr<TUdfObjectCodeCache> Cache;

        THolder()
        {
            auto config = New<TSlruCacheConfig>();
            config->Capacity = 64;
            Cache = New<TUdfObjectCodeCache>(config);
        }
    };
    static THolder holder;
    return holder.Cache.Get();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void AppendFunctionImplementation(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    bool functionIsAggregate,
    const std::string& functionName,
    const std::string& functionSymbolName,
    ECallingConvention functionCallingConvention,
    TSharedRef functionChunkSpecsFingerprint,
    TType functionRepeatedArgType,
    int functionRepeatedArgIndex,
    bool functionUseFunctionContext,
    const TQueryOptions& options,
    const TEnumIndexedArray<NCodegen::EExecutionBackend, TSharedRef>& implementationFiles)
{
    if (functionIsAggregate) {
        aggregateProfilers->emplace(functionName, New<TExternalAggregateCodegen>(
            functionName,
            implementationFiles,
            functionRepeatedArgIndex,
            functionRepeatedArgType,
            /*isFirst*/ false,
            functionChunkSpecsFingerprint));
    } else {
        NCodegen::IObjectCodePtr nativeObjectCode;
        if (options.AllowUdfObjectCodeCache) {
            TUdfObjectCodeKey key{
                .Fingerprint = std::string(functionChunkSpecsFingerprint.Begin(), functionChunkSpecsFingerprint.Size()),
                .FunctionName = functionName,
                .SymbolName = functionSymbolName,
            };
            // NB: Building native object code may fail in cmake builds because MCJIT's
            // addObjectFile does not properly expose symbols for JIT resolution.
            // In that case we fall back to loading from IR bitcode.
            try {
                nativeObjectCode = GetUdfObjectCodeCache()->FindOrInsert(key, [&] {
                    return MakeUdfNativeObjectCode(implementationFiles, functionName, {functionSymbolName});
                });
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to build native object code for UDF; falling back to IR bitcode (UDF: %Qv, Symbol: %v)",
                    functionName,
                    functionSymbolName);
            }
        }

        functionProfilers->emplace(functionName, New<TExternalFunctionCodegen>(
            functionName,
            functionSymbolName,
            implementationFiles,
            std::move(nativeObjectCode),
            functionCallingConvention,
            functionRepeatedArgType,
            functionRepeatedArgIndex,
            functionUseFunctionContext,
            functionChunkSpecsFingerprint));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
