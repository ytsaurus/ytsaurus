#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(Lock)
{
    Y_UNIT_TEST(TestNonwaitableLock)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        client->Create(workingDir + "/node_for_lock", NT_TABLE);
        auto tx1 = client->StartTransaction();
        auto lock = tx1->Lock(workingDir + "/node_for_lock", ELockMode::LM_EXCLUSIVE);

        auto getLockState = [&] {
            return client->Get("#" + GetGuidAsString(lock->GetId()) + "/@state").AsString();
        };

        UNIT_ASSERT(lock->GetAcquiredFuture().HasValue());
        UNIT_ASSERT_VALUES_EQUAL(getLockState(), "acquired");

        auto tx2 = client->StartTransaction();
        UNIT_ASSERT_EXCEPTION(
            tx2->Lock(workingDir + "/node_for_lock", ELockMode::LM_EXCLUSIVE),
            TErrorResponse
        );
    }

    Y_UNIT_TEST(TestWaitableOption)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        client->Create(workingDir + "/node_for_lock", NT_TABLE);
        auto tx1 = client->StartTransaction();
        auto tx2 = client->StartTransaction();
        tx1->Lock(workingDir + "/node_for_lock", ELockMode::LM_EXCLUSIVE);

        auto lockId = tx2->Lock(workingDir + "/node_for_lock", LM_EXCLUSIVE, TLockOptions().Waitable(true))->GetId();

        auto getLockState = [&] {
            return client->Get("#" + GetGuidAsString(lockId) + "/@state").AsString();
        };

        UNIT_ASSERT_VALUES_EQUAL(getLockState(), "pending");
        tx1->Abort();
        UNIT_ASSERT_VALUES_EQUAL(getLockState(), "acquired");
    }

    Y_UNIT_TEST(TestWait)
    {
        TZeroWaitLockPollIntervalGuard g;

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        client->Create(workingDir + "/node_for_lock", NT_TABLE);
        auto tx1 = client->StartTransaction();
        auto tx2 = client->StartTransaction();
        tx1->Lock(workingDir + "/node_for_lock", ELockMode::LM_EXCLUSIVE);

        auto lock = tx2->Lock(workingDir + "/node_for_lock", LM_EXCLUSIVE, TLockOptions().Waitable(true));
        auto lockAcquired = lock->GetAcquiredFuture();
        UNIT_ASSERT(!lockAcquired.Wait(TDuration::MilliSeconds(500)));
        tx1->Abort();
        lockAcquired.GetValue(TDuration::MilliSeconds(500));
    }

    Y_UNIT_TEST(TestBrokenWait)
    {
        TZeroWaitLockPollIntervalGuard g;

        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        client->Create(workingDir + "/node_for_lock", NT_TABLE);
        auto tx1 = client->StartTransaction();
        auto tx2 = client->StartTransaction();
        tx1->Lock(workingDir + "/node_for_lock", ELockMode::LM_EXCLUSIVE);

        auto lock = tx2->Lock(workingDir + "/node_for_lock", LM_EXCLUSIVE, TLockOptions().Waitable(true));
        auto lockAcquired = lock->GetAcquiredFuture();
        UNIT_ASSERT(!lockAcquired.Wait(TDuration::MilliSeconds(500)));
        tx2->Abort();
        // Don't want test to be flaky so use increased wait interval.
        UNIT_ASSERT(lockAcquired.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_EXCEPTION(lockAcquired.GetValue(), TErrorResponse);
    }

    Y_UNIT_TEST(TestChildKey)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        client->Create(workingDir + "/map-node", NT_MAP);
        client->Set(workingDir + "/map-node/child1", 1);
        client->Set(workingDir + "/map-node/child2", 2);

        auto tx1 = client->StartTransaction();

        // wrong lock type
        UNIT_ASSERT_EXCEPTION(
            tx1->Lock(workingDir + "/map-node", ELockMode::LM_EXCLUSIVE, TLockOptions().ChildKey("child1")),
            TErrorResponse);

        // should be ok
        tx1->Lock(workingDir + "/map-node", ELockMode::LM_SHARED, TLockOptions().ChildKey("child1"));

        tx1->Set(workingDir + "/map-node/child1", 11);

        UNIT_ASSERT_EXCEPTION(
            tx1->Lock(workingDir + "/map-node", ELockMode::LM_EXCLUSIVE, TLockOptions().ChildKey("non-existent-key")),
            TErrorResponse);

        auto tx2 = client->StartTransaction();

        // locked
        UNIT_ASSERT_EXCEPTION(tx2->Set(workingDir + "/map-node/child1", 12), TErrorResponse);

        // lock is already taken
        UNIT_ASSERT_EXCEPTION(
            tx2->Lock(workingDir + "/map-node", ELockMode::LM_SHARED, TLockOptions().ChildKey("child1")),
            TErrorResponse);

        // should be ok
        tx2->Lock(workingDir + "/map-node", ELockMode::LM_SHARED, TLockOptions().ChildKey("child2"));
        tx2->Set(workingDir + "/map-node/child2", 22);
    }

    Y_UNIT_TEST(TestAttributeKey)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        client->Create(workingDir + "/table", NT_TABLE);
        client->Set(workingDir + "/table/@attribute1", 1);
        client->Set(workingDir + "/table/@attribute2", 2);

        auto tx1 = client->StartTransaction();

        // wrong lock type
        UNIT_ASSERT_EXCEPTION(
            tx1->Lock(workingDir + "/table",
                ELockMode::LM_EXCLUSIVE,
                TLockOptions().AttributeKey("attribute1")),
            TErrorResponse);

        // should be ok
        tx1->Lock(workingDir + "/table",
            ELockMode::LM_SHARED,
            TLockOptions().ChildKey("attribute1"));

        tx1->Set(workingDir + "/table/@attribute1", 11);

        auto tx2 = client->StartTransaction();

        // lock is already taken
        UNIT_ASSERT_EXCEPTION(
            tx2->Lock(workingDir + "/table",
                ELockMode::LM_SHARED,
                TLockOptions().ChildKey("attribute1")),
            TErrorResponse);

        UNIT_ASSERT_EXCEPTION(
            tx2->Set(workingDir + "/table/@attribute1", 12),
            TErrorResponse);
    }

    Y_UNIT_TEST(TestGetLockedNodeId)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();

        client->Create(workingDir + "/node_for_lock", NT_TABLE);
        auto tx = client->StartTransaction();
        auto lock = tx->Lock(workingDir + "/node_for_lock", ELockMode::LM_EXCLUSIVE, TLockOptions().Waitable(true));
        auto expectedId = GetGuid(tx->Get(workingDir + "/node_for_lock/@id").AsString());

        UNIT_ASSERT_VALUES_EQUAL(lock->GetLockedNodeId(), expectedId);
    }

    Y_UNIT_TEST(TestUnlock)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        TYPath path = workingDir + "/node";

        client->Set("//sys/@config/cypress_manager/enable_unlock_command", true);

        client->Set(path, 1);
        auto tx = client->StartTransaction();
        auto lock = tx->Lock(path, ELockMode::LM_EXCLUSIVE);

        auto otherTx = client->StartTransaction();
        UNIT_ASSERT_EXCEPTION(otherTx->Set(path, 2), TErrorResponse);

        tx->Unlock(path);

        UNIT_ASSERT_NO_EXCEPTION(otherTx->Set(path, 2));
        UNIT_ASSERT_VALUES_EQUAL(otherTx->Get(path).AsInt64(), 2);

        // No exception when unlocking node without locks.
        UNIT_ASSERT_NO_EXCEPTION(tx->Unlock(path));
    }
}

////////////////////////////////////////////////////////////////////////////////
