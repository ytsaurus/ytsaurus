#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const TConstFunctionProfilerMapPtr GetBuiltinFunctionProfilers()
{
    struct TStorage
    {
        const TConstFunctionProfilerMapPtr Profilers = New<TFunctionProfilerMap>();
    };

    return LeakySingleton<TStorage>()->Profilers;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
