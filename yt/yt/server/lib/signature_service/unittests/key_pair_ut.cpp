#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/signature_service/key_pair.h>

#include <random>

namespace NYT::NSignatureService {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

TKeyPairMetadata SimpleMetadata(auto createdDelta, auto validAfterDelta, auto expiresAtDelta)
{
    // TODO(pavook) mock time provider.

    auto now = Now();
    return {
        .Owner = TOwnerId("test"),
        .Id = TKeyId(TGuid::Create()),
        .CreatedAt = now + createdDelta,
        .ValidAfter = now + validAfterDelta,
        .ExpiresAt = now + expiresAtDelta,
    };
}

////////////////////////////////////////////////////////////////////////////////

struct TRandomByteGenerator
{
    std::mt19937 rnd;

    TRandomByteGenerator() = default;

    TRandomByteGenerator(auto seedValue)
        : rnd(seedValue)
    {
    }

    std::byte operator()()
    {
        return static_cast<std::byte>(rnd());
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyPairMetadata, IsValid)
{
    {
        auto metaOk = SimpleMetadata(-10h, -10h, 10h);
        EXPECT_TRUE(metaOk.IsValid());
    }

    {
        auto metaExpired = SimpleMetadata(-10h, -10h, -5h);
        EXPECT_FALSE(metaExpired.IsValid());
    }

    {
        auto metaNotYetValid = SimpleMetadata(-10h, 5h, 10h);
        EXPECT_FALSE(metaNotYetValid.IsValid());
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyInfo, Verify)
{
    InitializeCryptography();

    auto metaOk = SimpleMetadata(-10h, -10h, 10h);
    EXPECT_TRUE(metaOk.IsValid());

    std::array<std::byte, 10> randomData;
    std::ranges::generate(randomData, TRandomByteGenerator());

    TPublicKey publicKey;
    std::array<std::byte, SignatureSize> signature;

    // Random key, random signature, valid meta.
    {
        std::ranges::generate(publicKey, TRandomByteGenerator());

        std::ranges::generate(signature, TRandomByteGenerator());

        auto keyInfo = New<TKeyInfo>(publicKey, metaOk);
        EXPECT_FALSE(keyInfo->Verify(randomData, signature));
    }

    std::array<std::byte, PrivateKeySize> privateKey;
    EXPECT_EQ(
        crypto_sign_keypair(
            reinterpret_cast<unsigned char*>(publicKey.data()),
            reinterpret_cast<unsigned char*>(privateKey.data())),
        0);

    // Valid key, random signature, valid meta.
    {
        auto keyInfo = New<TKeyInfo>(publicKey, metaOk);
        EXPECT_FALSE(keyInfo->Verify(randomData, signature));
    }

    crypto_sign_detached(
        reinterpret_cast<unsigned char*>(signature.data()),
        nullptr,
        reinterpret_cast<const unsigned char*>(randomData.data()),
        randomData.size(),
        reinterpret_cast<const unsigned char*>(privateKey.data()));

    // Valid key, valid signature, valid meta.
    {
        auto keyInfo = New<TKeyInfo>(publicKey, metaOk);
        EXPECT_TRUE(keyInfo->Verify(randomData, signature));
    }

    // Valid key, valid signature, expired meta.
    {
        auto metaExpired = SimpleMetadata(-10h, -10h, -5h);
        auto keyInfo = New<TKeyInfo>(publicKey, metaExpired);
        EXPECT_FALSE(keyInfo->Verify(randomData, signature));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyPair, Construct)
{
    EXPECT_FALSE(std::copy_constructible<TKeyPair>);
    EXPECT_TRUE(std::move_constructible<TKeyPair>);

    InitializeCryptography();

    auto metaOk = SimpleMetadata(0h, -1h, 10h);
    TKeyPair keyPair(metaOk);

    EXPECT_EQ(keyPair.KeyInfo().Meta().Id, metaOk.Id);
    EXPECT_TRUE(keyPair.SanityCheck());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyPair, Sign)
{
    InitializeCryptography();

    auto metaOk = SimpleMetadata(0h, -1h, 10h);
    TKeyPair keyPair(metaOk);

    std::array<std::byte, 1234> randomData;
    std::ranges::generate(randomData, TRandomByteGenerator());

    std::array<std::byte, SignatureSize> signature;
    keyPair.Sign(randomData, signature);

    EXPECT_TRUE(keyPair.KeyInfo().Verify(randomData, signature));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignatureService
