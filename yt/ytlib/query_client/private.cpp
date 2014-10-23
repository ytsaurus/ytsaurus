#include "private.h"
#include "plan_fragment.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger QueryClientLogger("QueryClient");

NLog::TLogger BuildLogger(const TConstQueryPtr& query)
{
    NLog::TLogger result(QueryClientLogger);
    result.AddTag("FragmentId: %v", query->GetId());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

