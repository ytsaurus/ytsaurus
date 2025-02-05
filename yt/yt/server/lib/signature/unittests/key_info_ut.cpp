#include <yt/yt/core/test_framework/framework.h>

#include "helpers.h"

#include <yt/yt/server/lib/signature/key_pair.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

#include <contrib/libs/libsodium/include/sodium/randombytes.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace std::chrono_literals;

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyPairMetadataTest, IsValid)
{
    {
        auto metaOk = SimpleMetadata(-10h, -10h, 10h);
        EXPECT_TRUE(IsKeyPairMetadataValid(metaOk));
    }

    {
        auto metaExpired = SimpleMetadata(-10h, -10h, -5h);
        EXPECT_FALSE(IsKeyPairMetadataValid(metaExpired));
    }

    {
        auto metaNotYetValid = SimpleMetadata(-10h, 5h, 10h);
        EXPECT_FALSE(IsKeyPairMetadataValid(metaNotYetValid));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyPairMetadataTest, SerializeDeserialize)
{
    auto meta = SimpleMetadata(-10h, -10h, 10h);
    auto serialized = ConvertToYsonString(meta);
    EXPECT_EQ(meta, ConvertTo<TKeyPairMetadata>(serialized));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyPairMetadataTest, DeserializeFail)
{
    constexpr static TStringBuf MetadataString =
        R"({version="0.999";owner_id="owner";key_id="4-3-2-1";created_at="2024-10-21T16:19:41Z";)"
        R"(valid_after="2024-10-21T16:19:41Z";expires_at="2024-10-21T16:19:41Z";})";

    EXPECT_THROW(ConvertTo<TKeyPairMetadata>("{}"), std::exception);

    {
        auto node = ConvertToNode(TYsonString(MetadataString));
        node->AsMap()->GetChildOrThrow("version")->AsString()->SetValue("0.999");
        EXPECT_THROW_WITH_SUBSTRING(
            ConvertTo<TKeyPairMetadata>(node),
            "Unknown TKeyPair version");
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyInfoTest, SerializeDeserialize)
{
    TPublicKey key;
    std::generate(key.begin(), key.end(), TRandomByteGenerator{});

    auto keyInfoPtr = New<TKeyInfo>(key, SimpleMetadata(-10h, -10h, 10h));
    auto serialized = ConvertToYsonString(*keyInfoPtr);
    EXPECT_EQ(*keyInfoPtr, ConvertTo<TKeyInfo>(serialized));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyInfoTest, DeserializeWrongLength)
{
    TPublicKey key;
    std::generate(key.begin(), key.end(), TRandomByteGenerator{});

    auto serialized = ConvertToYsonString(New<TKeyInfo>(key, SimpleMetadata(-10h, -10h, 10h)));
    auto node = ConvertToNode(serialized);
    node->AsMap()->GetChildOrThrow("public_key")->AsString()->SetValue("abacaba");
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TKeyInfo>(node),
        "Received incorrect public key size");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyInfoTest, Verify)
{
    InitializeCryptography();

    auto metaOk = SimpleMetadata(-10h, -10h, 10h);
    EXPECT_TRUE(IsKeyPairMetadataValid(metaOk));

    std::array<std::byte, 10> randomData;
    std::generate(randomData.begin(), randomData.end(), TRandomByteGenerator());

    TPublicKey publicKey;
    std::array<std::byte, SignatureSize> signature;

    // Random key, random signature, valid meta.
    {
        std::generate(publicKey.begin(), publicKey.end(), TRandomByteGenerator());

        std::generate(signature.begin(), signature.end(), TRandomByteGenerator());

        auto keyInfo = New<TKeyInfo>(publicKey, metaOk);
        EXPECT_FALSE(keyInfo->Verify(randomData, signature));
    }

    std::array<std::byte, PrivateKeySize> privateKey;
    std::array<unsigned char, crypto_sign_ed25519_SEEDBYTES> seed{};
    randombytes_buf(seed.data(), seed.size());
    EXPECT_EQ(
        crypto_sign_ed25519_seed_keypair(
            reinterpret_cast<unsigned char*>(publicKey.data()),
            reinterpret_cast<unsigned char*>(privateKey.data()),
            seed.data()),
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

} // namespace
} // namespace NYT::NSignature
