#include <yt/yt/tests/cpp/distributed_table_api_test.h>

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

// TODO(arkady-e1ppa): Uncomment this when signatures are in place.
// TEST_F(TDistributedTableSignaturesTest, FinishWithFakeSession)
// {
//     CreateStaticTable(
//         /*tablePath*/ "//tmp/distributed_table_api_test",
//         /*schema*/ "["
//         "{name=v1;type=string};"
//         "]");

//     {
//         TDistributedWriteSession session;
//         EXPECT_ANY_THROW(FinishDistributedWriteSession(
//             TSignedDistributedWriteSessionPtr(New<TSignature>(ConvertToYsonString(session)))));
//     }
// }

// TEST_F(TDistributedTableSignaturesTest, FinishWithFakeWriteResult)
// {
//     CreateStaticTable(
//         /*tablePath*/ "//tmp/distributed_table_api_test",
//         /*schema*/ "["
//         "{name=v1;type=string};"
//         "]");

//     auto sessionWithCookies = StartDistributedWriteSession(/*append*/ true, /*cookieCount*/ 0);

//     {
//         TWriteFragmentResult result;
//         EXPECT_ANY_THROW(FinishDistributedWriteSession(
//             sessionWithCookies.Session,
//             {TSignedWriteFragmentResultPtr(New<TSignature>(ConvertToYsonString(result)))}));
//     }
// }

// TEST_F(TDistributedTableSignaturesTest, WriteFragmentWithFakeCookie)
// {
//     CreateStaticTable(
//         /*tablePath*/ "//tmp/distributed_table_api_test",
//         /*schema*/ "["
//         "{name=v1;type=string};"
//         "]");

//     std::vector<TString> rowStrings = {
//         "<id=0>Foo;",
//     };

//     {
//         TWriteFragmentCookie cookie;
//         EXPECT_ANY_THROW(DistributedWriteTable(
//             TSignedWriteFragmentCookiePtr(New<TSignature>(ConvertToYsonString(cookie))),
//             {"v1"},
//             rowStrings));
//     }
// }

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT
