#include "framework.h"

#include <yt/core/misc/address.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAddressTest, StripInterconnectFromAddress)
{
    EXPECT_EQ("m01-sas-i.yt.yandex.net", StripInterconnectFromAddress("m02-sas-i.yt.yandex.net", "m01-sas-i.yt.yandex.net"));
    EXPECT_EQ("m01-sas-i.yt.yandex.net", StripInterconnectFromAddress("m02-i.yt.yandex.net", "m01-sas-i.yt.yandex.net"));
    EXPECT_EQ("m01-sas.yt.yandex.net", StripInterconnectFromAddress("m02-fol-i.yt.yandex.net", "m01-sas-i.yt.yandex.net"));
    EXPECT_EQ("m01-sas.yt.yandex.net", StripInterconnectFromAddress("m02-fol.yt.yandex.net", "m01-sas-i.yt.yandex.net"));
    EXPECT_EQ("127.0.0.1", StripInterconnectFromAddress("m02-fol-i.yt.yandex.net", "127.0.0.1"));
    EXPECT_EQ("banach", StripInterconnectFromAddress("m02-fol-i.yt.yandex.net", "banach"));
    EXPECT_EQ("banach", StripInterconnectFromAddress("m02-fol.yt.yandex.net", "banach"));
    EXPECT_EQ("s01-i.hahn", StripInterconnectFromAddress("m02-fol.yt.yandex.net", "s01-i.hahn"));
    EXPECT_EQ("s01-i.hahn", StripInterconnectFromAddress("n02-fol-i.yt.yandex.net", "s01-i.hahn"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

