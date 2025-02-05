#include <yt/yt/tests/cpp/distributed_table_api_test.h>

#include <yt/yt/client/signature/signature.h>
#include <yt/yt/client/signature/generator.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NCppTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NSignature;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TDistributedTableSignaturesTest
    : public TDistributedTableApiTest
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedTableSignaturesTest, FinishWithFakeSession)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "]");

    {
        TDistributedWriteSession session;
        session.PatchInfo.ChunkSchema = ConvertTo<TTableSchemaPtr>(TYsonString("[{name=v1;type=string};]"_sb));
        auto signedSession = CreateDummySignatureGenerator()->Sign(ConvertToYsonString(session));
        EXPECT_THROW_WITH_SUBSTRING(FinishDistributedWriteSession(
            TSignedDistributedWriteSessionPtr(std::move(signedSession))), "Signature validation failed");
    }
}

TEST_F(TDistributedTableSignaturesTest, FinishWithFakeWriteResult)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "]");

    auto sessionWithCookies = StartDistributedWriteSession(/*append*/ true, /*cookieCount*/ 0);

    {
        TWriteFragmentResult result;
        auto session = ConvertTo<TDistributedWriteSession>(sessionWithCookies.Session.Underlying()->Payload());
        result.SessionId = session.RootChunkListId;
        auto signedResult = CreateDummySignatureGenerator()->Sign(ConvertToYsonString(result));
        EXPECT_THROW_WITH_SUBSTRING(FinishDistributedWriteSession(
            sessionWithCookies.Session,
            {TSignedWriteFragmentResultPtr(std::move(signedResult))}), "Signature validation failed");
    }
}

TEST_F(TDistributedTableSignaturesTest, WriteFragmentWithFakeCookie)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "]");

    std::vector<TString> rowStrings = {
        "<id=0>Foo;",
    };

    {
        TWriteFragmentCookie cookie;
        cookie.PatchInfo.ChunkSchema = ConvertTo<TTableSchemaPtr>(TYsonString("[{name=v1;type=string};]"_sb));
        auto signedCookie = CreateDummySignatureGenerator()->Sign(ConvertToYsonString(cookie));
        EXPECT_THROW_WITH_SUBSTRING(DistributedWriteTable(
            TSignedWriteFragmentCookiePtr(std::move(signedCookie)),
            {"v1"},
            rowStrings), "Signature validation failed");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT
