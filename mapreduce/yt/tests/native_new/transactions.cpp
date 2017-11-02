#include "lib.h"

#include <mapreduce/yt/interface/client.h>

#include <library/unittest/registar.h>

#include <util/system/getpid.h>


using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

SIMPLE_UNIT_TEST_SUITE(Transactions)
{
    SIMPLE_UNIT_TEST(TestTitle)
    {
        auto client = CreateTestClient();
        auto getTitle = [&] (const ITransactionPtr& tx) {
            auto node = client->Get("//sys/transactions/" + GetGuidAsString(tx->GetId()) + "/@title");
            return node.AsString();
        };

        auto noTitleTx = client->StartTransaction();
        {
            auto pidStr = ToString(GetPID());
            auto title = getTitle(noTitleTx);
            UNIT_ASSERT(title.find(pidStr) != TString::npos);
        }

        auto titleTx = client->StartTransaction(TStartTransactionOptions().Title("foo"));
        UNIT_ASSERT_VALUES_EQUAL(getTitle(titleTx), "foo");

        auto attrTitleTx = client->StartTransaction(TStartTransactionOptions().Attributes(TNode()("title", "bar")));
        UNIT_ASSERT_VALUES_EQUAL(getTitle(attrTitleTx), "bar");
    }
}

////////////////////////////////////////////////////////////////////////////////
