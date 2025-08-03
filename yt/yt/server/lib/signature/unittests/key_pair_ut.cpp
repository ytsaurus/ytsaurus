#include <yt/yt/core/test_framework/framework.h>

#include "helpers.h"

#include <yt/yt/server/lib/signature/key_pair.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyPairTest, Construct)
{
    EXPECT_TRUE(std::copy_constructible<TKeyPair>);
    EXPECT_TRUE(std::move_constructible<TKeyPair>);

    InitializeCryptography();

    auto metaOk = SimpleMetadata(0h, -1h, 10h);
    auto keyPair = New<TKeyPair>(metaOk);

    EXPECT_EQ(
        GetKeyId(keyPair->KeyInfo()->Meta()),
        GetKeyId(metaOk));
    EXPECT_TRUE(keyPair->CheckSanity());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyPairTest, Sign)
{
    InitializeCryptography();

    auto metaOk = SimpleMetadata(0h, -1h, 10h);
    auto keyPair = New<TKeyPair>(metaOk);

    std::array<char, 1234> randomData;
    std::generate(randomData.begin(), randomData.end(), TRandomByteGenerator());

    std::array<char, SignatureSize> signature;
    keyPair->Sign(randomData, signature);

    EXPECT_TRUE(keyPair->KeyInfo()->Verify(randomData, signature));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
