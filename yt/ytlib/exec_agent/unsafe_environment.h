#pragma once

#include "public.h"
#include "environment.h"

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

class TUnsafeEnvironmentBuilder
    : public IEnvironmentBuilder
{
public:
    IProxyControllerPtr CreateProxyController(
        NYTree::INodePtr config, 
        const TJobId& jobId, 
        const Stroka& workingDirectory);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

