#pragma once

#include <yp/server/scheduler/node.h>
#include <yp/server/scheduler/pod.h>

namespace NYP::NServer::NScheduler::NTests {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TPod> CreateMockPod(ui64 cpuCapacity = 1000, ui64 memoryCapacity = 1024 * 1024);

std::unique_ptr<TNode> CreateMockNode(
    THomogeneousResource cpuResource,
    THomogeneousResource memoryResource);

std::unique_ptr<TNode> CreateMockNode();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler::NTests
