#pragma once

#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/pod.h>

namespace NYP::NServer::NCluster::NTests {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TPod> CreateMockPod(ui64 cpuCapacity = 1000, ui64 memoryCapacity = 1024 * 1024);

std::unique_ptr<TNode> CreateMockNode(
    THomogeneousResource cpuResource,
    THomogeneousResource memoryResource);

std::unique_ptr<TNode> CreateMockNode();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster::NTests
