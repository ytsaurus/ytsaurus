#include <yt/yt/core/test_framework/framework.h>

#include "helpers.h"
#include "stub_keystore.h"

#include <yt/yt/server/lib/signature/signature_generator.h>

#include <yt/yt/server/lib/signature/config.h>
#include <yt/yt/server/lib/signature/signature_header.h>
#include <yt/yt/server/lib/signature/signature_preprocess.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

using ::testing::IsNull;
using ::testing::Ne;
using ::testing::Pointee;

////////////////////////////////////////////////////////////////////////////////

struct TSignatureGeneratorTest
    : public ::testing::Test
{
    const TOwnerId OwnerId = TOwnerId("generator-test");
    const TStubKeyStorePtr Store;
    const TSignatureGeneratorConfigPtr Config;
    const TSignatureGeneratorPtr Gen;

    TKeyPairMetadata ValidKeyMeta()
    {
        return SimpleMetadata(
            -10h,
            -10h,
            10h,
            TKeyId(TGuid::Create()),
            OwnerId);
    }

    TSignatureGeneratorTest()
        : Store(New<TStubKeyStore>(OwnerId))
        , Config(New<TSignatureGeneratorConfig>())
        , Gen(New<TSignatureGenerator>(Config))
    { }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureGeneratorTest, SetKeyPair) {
    EXPECT_THAT(Gen->KeyInfo(), IsNull());

    auto keyPair = New<TKeyPair>(ValidKeyMeta());
    auto keyInfo = keyPair->KeyInfo();
    Gen->SetKeyPair(std::move(keyPair));
    auto firstKeyInfo = Gen->KeyInfo();
    EXPECT_THAT(firstKeyInfo, Pointee(*keyInfo));

    Gen->SetKeyPair(New<TKeyPair>(ValidKeyMeta()));
    EXPECT_THAT(Gen->KeyInfo(), Pointee(Ne(*keyInfo)));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureGeneratorTest, SimpleSign)
{
    auto keyPair = New<TKeyPair>(ValidKeyMeta());
    auto keyInfo = keyPair->KeyInfo();
    Gen->SetKeyPair(std::move(keyPair));

    std::string data("MyImportantData");
    auto signature = Gen->Sign(data);
    EXPECT_EQ(signature->Payload(), data);

    auto signatureYson = ConvertToNode(ConvertToYsonString(signature));
    auto headerString = signatureYson->AsMap()->GetChildValueOrThrow<TString>("header");
    auto header = ConvertTo<TSignatureHeader>(TYsonString(headerString));

    // Sanity check.
    EXPECT_EQ(
        std::visit([] (const auto& header_) {
            return TOwnerId(header_.Issuer);
        }, header),
        Store->GetOwner());

    EXPECT_TRUE(std::visit(
        [] (const auto& header_) {
            auto now = Now();
            return header_.ValidAfter < now && now < header_.ExpiresAt;
        },
        header));

    auto toSign = PreprocessSignature(TYsonString(headerString), signature->Payload());

    auto signatureNode = ConvertToNode(ConvertToYsonString(signature));
    auto signatureByteString = signatureNode->AsMap()->GetChildValueOrThrow<std::string>("signature");
    auto signatureBytes = std::as_bytes(std::span(signatureByteString));
    EXPECT_EQ(signatureBytes.size(), SignatureSize);

    EXPECT_TRUE(keyInfo->Verify(
        toSign,
        signatureBytes.template first<SignatureSize>()));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureGeneratorTest, UninitializedSign)
{
    EXPECT_TRUE(Store->Data.empty());

    std::string data("MyImportantData");
    EXPECT_THROW_WITH_SUBSTRING(Y_UNUSED(Gen->Sign(data)), "uninitialized generator");
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureGeneratorTest, RotationUnderLoad)
{
    Gen->SetKeyPair(New<TKeyPair>(ValidKeyMeta()));

    auto newKeyPair = New<TKeyPair>(ValidKeyMeta());

    std::string data("MyImportantData");
    std::atomic<bool> allStarted = {false};
    std::atomic<size_t> finishedCount = {0};

    auto signerTask = BIND([this, &data, &allStarted, &finishedCount] () {
        while (!allStarted.load());
        while (true) {
            Y_UNUSED(Gen->Sign(data));
            if (finishedCount.fetch_add(1) > 100'000) {
                break;
            }
        }
    });

    int readerThreads = 10;
    auto readerPool = CreateThreadPool(readerThreads, "reader");
    for (int i = 0; i < readerThreads; ++i) {
        signerTask.Via(readerPool->GetInvoker()).Run();
    }

    allStarted.store(true);
    while (finishedCount.load() == 0);
    Gen->SetKeyPair(std::move(newKeyPair));
    EXPECT_LE(finishedCount.load(), 1000u);
}

} // namespace
} // namespace NYT::NSignature
