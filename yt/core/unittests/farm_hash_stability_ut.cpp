#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/farm_hash.h>


namespace NYT {
namespace {

/* NB: This test intention is to provide a sanity check for stability
 * of FarmHash and FarmFingerprint functions.
 */

/////////////////////////////////////////////////////////////////////////////

TEST(TFarmHashTest, Test)
{
    static_assert(std::is_same<ui64, decltype(FarmHash(42ULL))>::value);
    EXPECT_EQ(17355217915646310598ULL, FarmHash(42ULL));

    TString buf = "MDwhat?";

    static_assert(std::is_same<ui64, decltype(FarmHash(buf.Data(), buf.Size()))>::value);
    EXPECT_EQ(4563404214573070240ULL, FarmHash(buf.Data(), buf.Size()));

    static_assert(std::is_same<ui64, decltype(FarmHash(buf.Data(), buf.Size(), 42))>::value);
    EXPECT_EQ(4649748257165393602ULL, FarmHash(buf.Data(), buf.Size(), 42ULL));
}

TEST(TFarmFingerprintTest, Test)
{
    static_assert(std::is_same<ui64, decltype(FarmFingerprint(42ULL))>::value);
    EXPECT_EQ(17355217915646310598ULL, FarmFingerprint(42ULL));

    TString buf = "MDwhat?";

    static_assert(std::is_same<ui64, decltype(FarmFingerprint(buf.Data(), buf.Size()))>::value);
    EXPECT_EQ(10997514911242581312ULL, FarmFingerprint(buf.Data(), buf.Size()));

    static_assert(std::is_same<ui64, decltype(FarmFingerprint(1234ULL, 5678ULL))>::value);
    EXPECT_EQ(16769064555670434975ULL, FarmFingerprint(1234ULL, 5678ULL));
}

/////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
