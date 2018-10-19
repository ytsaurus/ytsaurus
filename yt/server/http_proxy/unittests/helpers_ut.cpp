#include <yt/core/test_framework/framework.h>

#include <yt/server/http_proxy/helpers.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NHttpProxy {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TTestParseQuery, Sample)
{
    ParseQueryString("path[10]=0");

    EXPECT_THROW(ParseQueryString("path[10]=0&path[a]=1"), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NHttpProxy
} // namespace NYT
