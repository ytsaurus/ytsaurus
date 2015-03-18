#include "private.h"
#include "plan_fragment.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger QueryClientLogger("QueryClient");

NLogging::TLogger BuildLogger(const TConstQueryPtr& query)
{
    NLogging::TLogger result(QueryClientLogger);
    result.AddTag("FragmentId: %v", query->Id);
    return result;
}

size_t LogThreshold = 100;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

