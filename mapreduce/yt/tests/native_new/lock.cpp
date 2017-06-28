#include "lib.h"

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/http/error.h>

#include <library/unittest/registar.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

class TZeroWaitLockPollIntervalGuard
{
public:
    TZeroWaitLockPollIntervalGuard()
        : OldWaitLockPollInterval_(TConfig::Get()->WaitLockPollInterval)
    {
        TConfig::Get()->WaitLockPollInterval = TDuration::Zero();
    }

    ~TZeroWaitLockPollIntervalGuard()
    {
        TConfig::Get()->WaitLockPollInterval = OldWaitLockPollInterval_;
    }

private:
    TDuration OldWaitLockPollInterval_;
};

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
        UNIT_ASSERT(lockAcquired.Wait(TDuration::MilliSeconds(500)));
        UNIT_ASSERT_EXCEPTION(lockAcquired.GetValue(), TErrorResponse);
    }
}

////////////////////////////////////////////////////////////////////////////////
