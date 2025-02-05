#include <yt/yt/core/test_framework/framework.h>

#include "stub_keystore.h"

#include <yt/yt/server/lib/signature/signature_generator.h>

#include <yt/yt/server/lib/signature/config.h>
#include <yt/yt/server/lib/signature/signature_header.h>
#include <yt/yt/server/lib/signature/signature_preprocess.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TSignatureGeneratorTest
    : public ::testing::Test
{
    TStubKeyStorePtr Store;
    TSignatureGeneratorConfigPtr Config;
    TSignatureGeneratorPtr Gen;

    TSignatureGeneratorTest()
        : Store(New<TStubKeyStore>())
        , Config(New<TSignatureGeneratorConfig>())
        , Gen(New<TSignatureGenerator>(Config, Store))
    { }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureGeneratorTest, Rotate)
{
    WaitFor(Gen->Rotate()).ThrowOnError();
    EXPECT_EQ(Store->Data.size(), 1ULL);
    EXPECT_EQ(Store->Data[Store->GetOwner()].size(), 1ULL);
    EXPECT_EQ(*Store->Data[Store->GetOwner()][0], *Gen->KeyInfo());

    WaitFor(Gen->Rotate()).ThrowOnError();
    EXPECT_EQ(Store->Data.size(), 1ULL);
    EXPECT_EQ(Store->Data[Store->GetOwner()].size(), 2ULL);
    EXPECT_NE(*Store->Data[Store->GetOwner()][0], *Store->Data[Store->GetOwner()][1]);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureGeneratorTest, SimpleSign)
{
    WaitFor(Gen->Rotate()).ThrowOnError();

    auto data = ConvertToYsonString("MyImportantData");
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
    auto signatureByteString = signatureNode->AsMap()->GetChildValueOrThrow<TString>("signature");
    auto signatureBytes = std::as_bytes(std::span(TStringBuf(signatureByteString)));
    EXPECT_EQ(signatureBytes.size(), SignatureSize);

    EXPECT_TRUE(Store->Data[Store->GetOwner()][0]->Verify(
        toSign,
        signatureBytes.template first<SignatureSize>()));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureGeneratorTest, UninitializedSign)
{
    EXPECT_TRUE(Store->Data.empty());

    auto data = ConvertToYsonString("MyImportantData");
    EXPECT_THROW_WITH_SUBSTRING(Y_UNUSED(Gen->Sign(data)), "uninitialized generator");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
