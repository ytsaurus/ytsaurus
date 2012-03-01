#include "stdafx.h"

#include <ytlib/misc/thread_affinity.h>

#include <ytlib/actions/action_queue.h>
#include <ytlib/actions/action_util.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {
    class TMyObject
    {
        DECLARE_THREAD_AFFINITY_SLOT(FirstThread);
        DECLARE_THREAD_AFFINITY_SLOT(SecondThread);

    public:
        TVoid A() {
            VERIFY_THREAD_AFFINITY(FirstThread);
            return TVoid();
        }

        TVoid B() {
            VERIFY_THREAD_AFFINITY(SecondThread);
            return TVoid();
        }

        TVoid C() {
            VERIFY_THREAD_AFFINITY(FirstThread);
            return TVoid();
        }
    };

#define PROLOGUE() \
    auto queue1 = New<TActionQueue>(); \
    auto queue2 = New<TActionQueue>(); \
    auto invoker1 = queue1->GetInvoker(); \
    auto invoker2 = queue2->GetInvoker(); \

    void SingleThreadedAccess(TMyObject* object)
    {
        PROLOGUE();

        FromMethod(&TMyObject::A, object)->AsyncVia(invoker1)->Do()->Get();
        FromMethod(&TMyObject::B, object)->AsyncVia(invoker1)->Do()->Get();

        FromMethod(&TMyObject::A, object)->AsyncVia(invoker1)->Do()->Get();
        FromMethod(&TMyObject::B, object)->AsyncVia(invoker1)->Do()->Get();
    }

    void UntangledThreadAccess(TMyObject* object)
    {
        PROLOGUE();

        FromMethod(&TMyObject::A, object)->AsyncVia(invoker1)->Do()->Get();
        FromMethod(&TMyObject::B, object)->AsyncVia(invoker2)->Do()->Get();

        FromMethod(&TMyObject::A, object)->AsyncVia(invoker1)->Do()->Get();
        FromMethod(&TMyObject::B, object)->AsyncVia(invoker2)->Do()->Get();
    }

    void UntangledThreadAccessToSharedSlot(TMyObject* object)
    {
        PROLOGUE();

        FromMethod(&TMyObject::A, object)->AsyncVia(invoker1)->Do()->Get();
        FromMethod(&TMyObject::B, object)->AsyncVia(invoker2)->Do()->Get();
        FromMethod(&TMyObject::C, object)->AsyncVia(invoker1)->Do()->Get();

        FromMethod(&TMyObject::A, object)->AsyncVia(invoker1)->Do()->Get();
        FromMethod(&TMyObject::B, object)->AsyncVia(invoker2)->Do()->Get();
        FromMethod(&TMyObject::C, object)->AsyncVia(invoker1)->Do()->Get();
    }

    void TangledThreadAccess1(TMyObject* object)
    {
        PROLOGUE();

        FromMethod(&TMyObject::A, object)->AsyncVia(invoker1)->Do()->Get();
        FromMethod(&TMyObject::B, object)->AsyncVia(invoker2)->Do()->Get();

        FromMethod(&TMyObject::A, object)->AsyncVia(invoker1)->Do()->Get();
        FromMethod(&TMyObject::B, object)->AsyncVia(invoker1)->Do()->Get();
    }

    void TangledThreadAccess2(TMyObject* object)
    {
        PROLOGUE();

        FromMethod(&TMyObject::A, object)->AsyncVia(invoker1)->Do()->Get();
        FromMethod(&TMyObject::B, object)->AsyncVia(invoker2)->Do()->Get();

        FromMethod(&TMyObject::A, object)->AsyncVia(invoker2)->Do()->Get();
        FromMethod(&TMyObject::B, object)->AsyncVia(invoker2)->Do()->Get();
    }
#undef PROLOGUE
} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

TEST(TThreadAffinityTest, SingleThreadedAccess)
{
    TMyObject object;
    SingleThreadedAccess(&object);

    SUCCEED();
}

TEST(TThreadAffinityTest, UntangledThreadAccess)
{
    TMyObject object;
    UntangledThreadAccess(&object);

    SUCCEED();
}

TEST(TThreadAffinityTest, UntangledThreadAccessToSharedSlot)
{
    TMyObject object;
    UntangledThreadAccessToSharedSlot(&object);

    SUCCEED();
}

TEST(TThreadAffinityDeathTest, TangledThreadAccess1)
{
    TMyObject object;
    ASSERT_DEATH({ TangledThreadAccess1(&object); }, ".*");
}

TEST(TThreadAffinityDeathTest, TangledThreadAccess2)
{
    TMyObject object;
    ASSERT_DEATH({ TangledThreadAccess2(&object); }, ".*");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

