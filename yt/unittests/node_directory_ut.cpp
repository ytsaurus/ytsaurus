#include "framework.h"

#include <yt/core/misc/error.h>
#include <yt/ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NNodeTrackerClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSelectAddress, Test)
{
    const TAddressMap map{{"ipv4", "127.0.0.1:8081"}, {"ipv6", "::1:8081"}, {"default", "localhost:8081"}};

    EXPECT_EQ("::1:8081", SelectAddress(map, {"ipv6", "ipv4"}));
    EXPECT_EQ("127.0.0.1:8081", SelectAddress(map, {"ipv4", "ipv6"}));
    EXPECT_THROW(SelectAddress(map, {"wrong"}), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NNodeTrackerClient
} // namespace NYT

