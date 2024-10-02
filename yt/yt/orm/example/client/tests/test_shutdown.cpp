#include "common.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/shutdown.h>

namespace NYT::NOrm::NExample::NClient::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TNativeClientShutdownTestSuite
    : public TNativeClientTestSuite
{ };

TEST_F(TNativeClientShutdownTestSuite, Shutdown)
{
    std::vector<TFuture<TGetMastersResult>> futures;
    futures.reserve(100000);
    for (int i = 0; i < 100000; ++i) {
        futures.push_back(Client_->GetMasters());
    }
    NYT::Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NExample::NClient::NTests
