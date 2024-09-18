#include <yt/yt/core/test_framework/framework.h>

#include "mock/key_store.h"

#include <yt/yt/server/lib/signature_service/signature_generator.h>

#include <yt/yt/server/lib/signature_service/signature.h>
#include <yt/yt/server/lib/signature_service/signature_header.h>
#include <yt/yt/server/lib/signature_service/signature_preprocess.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignatureService {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::chrono_literals;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TSignatureGenerator, Rotate)
{
    TMockKeyStore store;
    TSignatureGenerator gen(&store);

    gen.Rotate();
    EXPECT_EQ(store.Data.size(), 1ULL);
    EXPECT_EQ(store.Data[store.GetOwner()].size(), 1ULL);
    EXPECT_EQ(*store.Data[store.GetOwner()][0], gen.KeyInfo());

    gen.Rotate();
    EXPECT_EQ(store.Data.size(), 1ULL);
    EXPECT_EQ(store.Data[store.GetOwner()].size(), 2ULL);
    EXPECT_NE(*store.Data[store.GetOwner()][0], *store.Data[store.GetOwner()][1]);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSignatureGenerator, SimpleSign)
{
    TMockKeyStore store;

    TSignatureGenerator gen(&store);
    gen.Rotate();

    auto data = ConvertToYsonString("MyImportantData");
    TSignature signature = gen.Sign(TYsonString(data));
    EXPECT_EQ(signature.Payload(), data);

    auto signatureYson = ConvertToNode(ConvertToYsonString(signature));
    auto headerString = signatureYson->AsMap()->GetChildValueOrThrow<TString>("header");
    auto header = ConvertTo<TSignatureHeader>(TYsonString(headerString));
    // Sanity check.
    EXPECT_EQ(
        std::visit([] (const auto& header_) {
            return TOwnerId(header_.Issuer);
        }, header),
        store.GetOwner());

    EXPECT_TRUE(std::visit(
        [] (const auto& header_) {
            auto now = Now();
            return header_.ValidAfter < now && now < header_.ExpiresAt;
        },
        header));

    auto toSign = PreprocessSignature(TYsonString(headerString), signature.Payload());

    auto signatureNode = ConvertToNode(ConvertToYsonString(signature));
    auto signatureByteString = signatureNode->AsMap()->GetChildValueOrThrow<TString>("signature");
    auto signatureBytes = std::as_bytes(std::span(TStringBuf(signatureByteString)));
    EXPECT_EQ(signatureBytes.size(), SignatureSize);

    EXPECT_TRUE(store.Data[store.GetOwner()][0]->Verify(
        toSign,
        signatureBytes.template first<SignatureSize>()));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSignatureGenerator, UninitializedSign)
{
    TMockKeyStore store;

    TSignatureGenerator gen(&store);
    EXPECT_TRUE(store.Data.empty());

    auto data = NYson::ConvertToYsonString("MyImportantData");
    EXPECT_THROW_WITH_SUBSTRING(std::ignore = gen.Sign(std::move(data)), "uninitialized generator");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignatureService
