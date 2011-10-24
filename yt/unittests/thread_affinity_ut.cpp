#include "../ytlib/misc/thread_affinity.h"

#include "../ytlib/actions/action_queue.h"
#include "../ytlib/actions/action_util.h"

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TThreadAffinityTest : public ::testing::Test
{ };

class TMyObject
{
    DECLARE_THREAD_AFFINITY_SLOT(ServiceThread);

public:
    TVoid F() {
        VERIFY_THREAD_AFFINITY(ServiceThread);
        return TVoid();
    }

    TVoid G() {
        VERIFY_THREAD_AFFINITY(ServiceThread);
        return TVoid();
    }
};

TEST_F(TThreadAffinityTest, OneThread)
{
    TMyObject myObject;
    TActionQueue::TPtr actionQueue = ~New<TActionQueue>();
    auto invoker = actionQueue->GetInvoker();

    FromMethod(&TMyObject::F, &myObject)->AsyncVia(invoker)->Do()->Get();
    FromMethod(&TMyObject::F, &myObject)->AsyncVia(invoker)->Do()->Get();
    FromMethod(&TMyObject::G, &myObject)->AsyncVia(invoker)->Do()->Get();
    FromMethod(&TMyObject::F, &myObject)->AsyncVia(invoker)->Do()->Get();
    FromMethod(&TMyObject::F, &myObject)->AsyncVia(invoker)->Do()->Get();
    FromMethod(&TMyObject::G, &myObject)->AsyncVia(invoker)->Do()->Get();

    SUCCEED();
}

TEST_F(TThreadAffinityTest, OneFunctionDifferentThreads)
{
    TMyObject myObject;
    ASSERT_DEATH({
        TActionQueue::TPtr actionQueue1 = ~New<TActionQueue>();
        TActionQueue::TPtr actionQueue2 = ~New<TActionQueue>();
        FromMethod(&TMyObject::F, &myObject)->AsyncVia(actionQueue1->GetInvoker())->Do()->Get();
        FromMethod(&TMyObject::F, &myObject)->AsyncVia(actionQueue2->GetInvoker())->Do()->Get();
    }, ".*");

}

TEST_F(TThreadAffinityTest, DifferentFunctionsDifferentThreads)
{
    TMyObject myObject;
    ASSERT_DEATH({
        TActionQueue::TPtr actionQueue1 = ~New<TActionQueue>();
        TActionQueue::TPtr actionQueue2 = ~New<TActionQueue>();
        FromMethod(&TMyObject::F, &myObject)->AsyncVia(actionQueue1->GetInvoker())->Do()->Get();
        FromMethod(&TMyObject::G, &myObject)->AsyncVia(actionQueue2->GetInvoker())->Do()->Get();
    }, ".*");
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

