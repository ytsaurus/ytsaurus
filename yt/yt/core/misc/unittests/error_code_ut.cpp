#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/string/format.h>

#include <ostream>

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((Global1) (-5))
    ((Global2) (-6))
);

namespace NExternalWorld {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((X) (-11))
    ((Y) (-22))
    ((Z) (-33))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NExternalWorld

namespace NYT {

void PrintTo(const TErrorCodeRegistry::TErrorCodeInfo& errorCodeInfo, std::ostream* os)
{
    *os << ToString(errorCodeInfo);
}

namespace NInternalLittleWorld {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((A) (-1))
    ((B) (-2))
    ((C) (-3))
    ((D) (-4))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMyOwnLittleWorld

namespace {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((Kek)     (-57))
    ((Haha)    (-179))
    ((Muahaha) (-1543))
    ((Kukarek) (-2007))
);

TEST(TErrorCodeRegistryTest, Basic)
{
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-1543),
        (TErrorCodeRegistry::TErrorCodeInfo{"NYT::(anonymous namespace)", "Muahaha"}));
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-3),
        (TErrorCodeRegistry::TErrorCodeInfo{"NYT::NInternalLittleWorld", "C"}));
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-33),
        (TErrorCodeRegistry::TErrorCodeInfo{"NExternalWorld", "Z"}));
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-5),
        (TErrorCodeRegistry::TErrorCodeInfo{"", "Global1"}));
    EXPECT_EQ(
        TErrorCodeRegistry::Get()->Get(-111),
        (TErrorCodeRegistry::TErrorCodeInfo{"NUnknown", "ErrorCode-111"}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
