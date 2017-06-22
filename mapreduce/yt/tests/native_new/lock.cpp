#include "lib.h"

#include <mapreduce/yt/interface/client.h>

#include <library/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

SIMPLE_UNIT_TEST_SUITE(Lock)
{
    SIMPLE_UNIT_TEST(TestWaitableLock)
    {
        auto client = CreateTestClient();
        client->Create("//testing/node_for_lock", NT_TABLE);
        auto tx1 = client->StartTransaction();
        auto tx2 = client->StartTransaction();
        tx1->Lock("//testing/node_for_lock", ELockMode::LM_EXCLUSIVE);

        auto lockId = tx2->Lock("//testing/node_for_lock", LM_EXCLUSIVE, TLockOptions().Waitable(true))->GetId();

        auto getLockState = [&] {
            return tx2->Get("//sys/locks/" + GetGuidAsString(lockId) + "/@state").AsString();
        };

        UNIT_ASSERT_VALUES_EQUAL(getLockState(), "pending");
        tx1->Abort();
        UNIT_ASSERT_VALUES_EQUAL(getLockState(), "acquired");
    }
}

////////////////////////////////////////////////////////////////////////////////
