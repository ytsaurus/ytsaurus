#include <yt/yt/core/test_framework/framework.h>

#include "helpers.h"
#include "stub_keystore.h"

#include <yt/yt/server/lib/signature/signature_generator.h>

#include <yt/yt/server/lib/signature/config.h>
#include <yt/yt/server/lib/signature/signature_header.h>
#include <yt/yt/server/lib/signature/signature_preprocess.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;
using namespace NApi::NRpcProxy;
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
    {
        WaitFor(InitializeCryptography(GetCurrentInvoker()))
            .ThrowOnError();
    }
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
    auto headerString = signatureYson->AsMap()->GetChildValueOrThrow<std::string>("header");
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
    auto signatureString = signatureNode->AsMap()->GetChildValueOrThrow<std::string>("signature");
    EXPECT_EQ(signatureString.size(), SignatureSize);

    EXPECT_TRUE(keyInfo->Verify(
        toSign,
        std::span<const char, SignatureSize>(signatureString)));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureGeneratorTest, UninitializedSign)
{
    EXPECT_TRUE(Store->Data.empty());

    std::string data("MyImportantData");
    EXPECT_THROW_WITH_SUBSTRING(Y_UNUSED(Gen->Sign(data)), "not ready yet");

    try {
        Y_UNUSED(Gen->Sign(data));
    } catch(const std::exception& exception) {
        EXPECT_TRUE(IsRetriableError(exception));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureGeneratorTest, RotationUnderLoad)
{
    Gen->SetKeyPair(New<TKeyPair>(ValidKeyMeta()));

    auto newKeyPair = New<TKeyPair>(ValidKeyMeta());

    std::string data("MyImportantData");
    std::atomic<bool> allStarted = false;
    std::atomic<size_t> finishedCount = 0;

    auto signerTask = BIND([this, &data, &allStarted, &finishedCount] () {
        while (!allStarted.load()) { }
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
    while (finishedCount.load() == 0) { }
    Gen->SetKeyPair(std::move(newKeyPair));
    EXPECT_LE(finishedCount.load(), 5000u);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureGeneratorTest, ReconfigureChangesExpirationDelta)
{
    Gen->SetKeyPair(New<TKeyPair>(ValidKeyMeta()));

    auto newConfig = New<TSignatureGeneratorConfig>();
    newConfig->SignatureExpirationDelta = TDuration::Hours(3);
    newConfig->TimeSyncMargin = TDuration::Minutes(5);
    Gen->Reconfigure(newConfig);

    auto now = Now();
    auto signature = Gen->Sign("data");
    auto header = ConvertTo<TSignatureHeader>(
        TYsonString(TStringBuf(
            ConvertToNode(signature)->AsMap()
                ->GetChildValueOrThrow<std::string>("header"))));

    auto expiresAt = std::visit([](const auto& h) { return h.ExpiresAt; }, header);
    auto validAfter = std::visit([](const auto& h) { return h.ValidAfter; }, header);

    EXPECT_GE(expiresAt - now, TDuration::Hours(3) - TDuration::Seconds(10));
    EXPECT_LE(expiresAt - now, TDuration::Hours(3) + TDuration::Seconds(10));

    EXPECT_GE(now - validAfter, TDuration::Minutes(5) - TDuration::Seconds(10));
    EXPECT_LE(now - validAfter, TDuration::Minutes(5) + TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureGeneratorTest, ReconfigureMultipleTimes)
{
    Gen->SetKeyPair(New<TKeyPair>(ValidKeyMeta()));

    std::atomic<bool> allStarted = false;
    std::atomic<bool> shouldStop = false;
    std::atomic<size_t> signCount = 0;

    auto signerTask = BIND([this, &allStarted, &shouldStop, &signCount] () {
        while (!allStarted.load()) { }
        while (!shouldStop.load()) {
            auto signature = Gen->Sign("data");
            EXPECT_EQ(signature->Payload(), "data");
            signCount.fetch_add(1);
        }
    });

    auto reconfigureTask = BIND([this, &allStarted, &shouldStop] () {
        while (!allStarted.load()) { }
        for (int i = 0; i < 100 && !shouldStop.load(); ++i) {
            auto newConfig = New<TSignatureGeneratorConfig>();
            newConfig->SignatureExpirationDelta = TDuration::Hours(1 + i % 3);
            newConfig->TimeSyncMargin = TDuration::Minutes(10 + i % 5);
            Gen->Reconfigure(newConfig);
            Sleep(TDuration::MilliSeconds(5));
        }
    });

    auto threadPool = CreateThreadPool(2, "test");
    signerTask.Via(threadPool->GetInvoker()).Run();
    reconfigureTask.Via(threadPool->GetInvoker()).Run();

    allStarted.store(true);
    Sleep(TDuration::Seconds(3));
    shouldStop.store(true);

    EXPECT_GE(signCount.load(), 1000u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
