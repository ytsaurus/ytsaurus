#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/signature_service/signature.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignatureService {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TSignature, DeserializeSerialize)
{
    // SignatureSize bytes.
    TYsonString ysonOK(
        R"({"header"="header";"payload"="payload";"signature"=")" +
        TString(SignatureSize, 'X') +
        R"(";})"
    );

    TSignature signature;
    EXPECT_NO_THROW(signature = ConvertTo<TSignature>(ysonOK));
    EXPECT_EQ(signature.Payload().ToString(), "payload");

    EXPECT_EQ(ConvertToYsonString(signature, EYsonFormat::Text).ToString(), ysonOK.ToString());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSignature, DeserializeFail)
{
    {
        TYsonString ysonFail(
            R"({"header"="header";"payload"="payload";"signature"="abacaba";})"_sb
        );
        EXPECT_THROW_WITH_SUBSTRING(ConvertTo<TSignature>(ysonFail), "incorrect signature size");
    }
    {
        TYsonString ysonFail(
            R"({"header"="header";"buddy"="payload";"signature"="abacaba";})"_sb
        );
        EXPECT_THROW_WITH_SUBSTRING(ConvertTo<TSignature>(ysonFail), "no child with key \"payload\"");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignatureService
