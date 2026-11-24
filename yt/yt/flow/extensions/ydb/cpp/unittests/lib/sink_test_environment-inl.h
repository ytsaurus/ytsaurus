#ifndef SINK_TEST_ENVIRONMENT_INL_H_
    #error "Direct inclusion of this file is not allowed, include sink_test_environment.h"
    #include "sink_test_environment.h"
#endif

namespace NYT::NFlow::NYdbTesting {

////////////////////////////////////////////////////////////////////////////////

template <class TSink>
std::shared_ptr<int> TSinkTestEnvironment::Distribute(
    const TIntrusivePtr<TSink>& sink,
    const TOutputMessageConstPtr& message) const
{
    auto callbackCount = std::make_shared<int>(0);
    auto tracker = TDistributingTracker([callbackCount] {
        ++*callbackCount;
    });
    sink->Distribute(message, tracker.AddDestination());
    tracker.Activate();
    return callbackCount;
}

template <class TSink>
void TSinkTestEnvironment::Sync(const TIntrusivePtr<TSink>& sink, bool persist) const
{
    sink->PreSyncFlush();
    sink->Sync(/*transaction*/ nullptr);
    if (persist) {
        StateManager->Sync();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbTesting
