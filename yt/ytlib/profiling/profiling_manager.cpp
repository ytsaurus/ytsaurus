#include "stdafx.h"
#include "profiling_manager.h"

namespace NYT {
namespace NProfiling  {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TProfilingManager* TProfilingManager::Get()
{
    return Singleton<TProfilingManager>();
}

void TProfilingManager::AddSample(const TSample& sample)
{

}


////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
