#include "../ytlib/misc/thread_affinity_checker.h"

#include "../ytlib/actions/action_queue.h"
#include "../ytlib/actions/action_util.h"

#include "framework/framework.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TThreadAffinityTest : public ::testing::Test
{ };

class TMyObject
{
    DECLARE_THREAD_AFFINITY_SLOT(ServiceThread);

public:
    TVoid F() {
        Cout << "function f is called" << Endl;
        THREAD_AFFINITY_ONLY(ServiceThread);
    }

    TVoid G() {
        Cout << "function g is called" << Endl;
        THREAD_AFFINITY_ONLY(ServiceThread);
    }
};

TEST_F(TThreadAffinityTest, OneThread)
{
    TMyObject myObject;
    IInvoker::TPtr invoker = ~New<TActionQueue>();

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
    IInvoker::TPtr invoker1 = ~New<TActionQueue>();
    IInvoker::TPtr invoker2 = ~New<TActionQueue>();

    FromMethod(&TMyObject::F, &myObject)->AsyncVia(invoker1)->Do()->Get();
    ASSERT_DEATH({
        FromMethod(&TMyObject::F, &myObject)->AsyncVia(invoker2)->Do()->Get();
    }, ".*");

}

TEST_F(TThreadAffinityTest, DifferentFunctionsDifferentThreads)
{
    TMyObject myObject;
    IInvoker::TPtr invoker1 = ~New<TActionQueue>();
    IInvoker::TPtr invoker2 = ~New<TActionQueue>();

    FromMethod(&TMyObject::F, &myObject)->AsyncVia(invoker1)->Do()->Get();
    ASSERT_DEATH({
        FromMethod(&TMyObject::G, &myObject)->AsyncVia(invoker2)->Do()->Get();
    }, ".*");
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

