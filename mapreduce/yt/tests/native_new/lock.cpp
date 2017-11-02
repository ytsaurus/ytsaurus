#include "lib.h"

#include <mapreduce/yt/interface/client.h>

#include <library/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

SIMPLE_UNIT_TEST_SUITE(Lock)
{
    SIMPLE_UNIT_TEST(TestNonwaitableLock)
    {
        auto client = CreateTestClient();
        client->Create("//testing/node_for_lock", NT_TABLE);
        auto tx1 = client->StartTransaction();
        auto lock = tx1->Lock("//testing/node_for_lock", ELockMode::LM_EXCLUSIVE);

        auto getLockState = [&] {
            return client->Get("//sys/locks/" + GetGuidAsString(lock->GetId()) + "/@state").AsString();
        };

        UNIT_ASSERT(lock->GetAcquiredFuture().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(getLockState(), "acquired");

        auto tx2 = client->StartTransaction();
        UNIT_ASSERT_EXCEPTION(
            tx2->Lock("//testing/node_for_lock", ELockMode::LM_EXCLUSIVE),
            TErrorResponse
        );
    }

    SIMPLE_UNIT_TEST(TestWaitableOption)
    {
        auto client = CreateTestClient();
        client->Create("//testing/node_for_lock", NT_TABLE);
        auto tx1 = client->StartTransaction();
        auto tx2 = client->StartTransaction();
        tx1->Lock("//testing/node_for_lock", ELockMode::LM_EXCLUSIVE);

        auto lockId = tx2->Lock("//testing/node_for_lock", LM_EXCLUSIVE, TLockOptions().Waitable(true))->GetId();

        auto getLockState = [&] {
            return client->Get("//sys/locks/" + GetGuidAsString(lockId) + "/@state").AsString();
        };

        UNIT_ASSERT_VALUES_EQUAL(getLockState(), "pending");
        tx1->Abort();
        UNIT_ASSERT_VALUES_EQUAL(getLockState(), "acquired");
    }

    SIMPLE_UNIT_TEST(TestWait)
    {
        TZeroWaitLockPollIntervalGuard g;

        auto client = CreateTestClient();
        client->Create("//testing/node_for_lock", NT_TABLE);
        auto tx1 = client->StartTransaction();
        auto tx2 = client->StartTransaction();
        tx1->Lock("//testing/node_for_lock", ELockMode::LM_EXCLUSIVE);

        auto lock = tx2->Lock("//testing/node_for_lock", LM_EXCLUSIVE, TLockOptions().Waitable(true));
        auto lockAcquired = lock->GetAcquiredFuture();
        UNIT_ASSERT(!lockAcquired.Wait(TDuration::MilliSeconds(500)));
        tx1->Abort();
        lockAcquired.GetValue(TDuration::MilliSeconds(500));
    }

    SIMPLE_UNIT_TEST(TestBrokenWait)
    {
        TZeroWaitLockPollIntervalGuard g;

        auto client = CreateTestClient();
        client->Create("//testing/node_for_lock", NT_TABLE);
        auto tx1 = client->StartTransaction();
        auto tx2 = client->StartTransaction();
        tx1->Lock("//testing/node_for_lock", ELockMode::LM_EXCLUSIVE);

        auto lock = tx2->Lock("//testing/node_for_lock", LM_EXCLUSIVE, TLockOptions().Waitable(true));
        auto lockAcquired = lock->GetAcquiredFuture();
        UNIT_ASSERT(!lockAcquired.Wait(TDuration::MilliSeconds(500)));
        tx2->Abort();
        // Don't want test to be flaky so use increased wait interval.
        UNIT_ASSERT(lockAcquired.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_EXCEPTION(lockAcquired.GetValue(), TErrorResponse);
    }

    SIMPLE_UNIT_TEST(TestChildKey)
    {
        auto client = CreateTestClient();

        client->Create("//testing/map-node", NT_MAP);
        client->Set("//testing/map-node/child1", 1);
        client->Set("//testing/map-node/child2", 2);

        auto tx1 = client->StartTransaction();

        // wrong lock type
        UNIT_ASSERT_EXCEPTION(
            tx1->Lock("//testing/map-node", ELockMode::LM_EXCLUSIVE, TLockOptions().ChildKey("child1")),
            TErrorResponse);

        // should be ok
        tx1->Lock("//testing/map-node", ELockMode::LM_SHARED, TLockOptions().ChildKey("child1"));

        tx1->Set("//testing/map-node/child1", 11);

        UNIT_ASSERT_EXCEPTION(
            tx1->Lock("//testing/map-node", ELockMode::LM_EXCLUSIVE, TLockOptions().ChildKey("non-existent-key")),
            TErrorResponse);

        auto tx2 = client->StartTransaction();

        // locked
        UNIT_ASSERT_EXCEPTION(tx2->Set("//testing/map-node/child1", 12), TErrorResponse);

        // lock is already taken
        UNIT_ASSERT_EXCEPTION(
            tx2->Lock("//testing/map-node", ELockMode::LM_SHARED, TLockOptions().ChildKey("child1")),
            TErrorResponse);

        // should be ok
        tx2->Lock("//testing/map-node", ELockMode::LM_SHARED, TLockOptions().ChildKey("child2"));
        tx2->Set("//testing/map-node/child2", 22);
    }

    SIMPLE_UNIT_TEST(TestAttributeKey)
    {
        auto client = CreateTestClient();

        client->Create("//testing/table", NT_TABLE);
        client->Set("//testing/table/@attribute1", 1);
        client->Set("//testing/table/@attribute2", 2);

        auto tx1 = client->StartTransaction();

        // wrong lock type
        UNIT_ASSERT_EXCEPTION(
            tx1->Lock("//testing/table",
                ELockMode::LM_EXCLUSIVE,
                TLockOptions().AttributeKey("attribute1")),
            TErrorResponse);

        // should be ok
        tx1->Lock("//testing/table",
            ELockMode::LM_SHARED,
            TLockOptions().ChildKey("attribute1"));

        tx1->Set("//testing/table/@attribute1", 11);

        auto tx2 = client->StartTransaction();

        // lock is already taken
        UNIT_ASSERT_EXCEPTION(
            tx2->Lock("//testing/table",
                ELockMode::LM_SHARED,
                TLockOptions().ChildKey("attribute1")),
            TErrorResponse);

        UNIT_ASSERT_EXCEPTION(
            tx2->Set("//testing/table/@attribute1", 12),
            TErrorResponse);
    }
}

////////////////////////////////////////////////////////////////////////////////
