#pragma once

#include <yt/core/test_framework/framework.h>

#include <yt/server/election/election_manager.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionCallbacksMock
    : public IElectionCallbacks
{
public:
    MOCK_METHOD1(OnStartLeading, void(TEpochContextPtr epochContext));
    MOCK_METHOD0(OnStopLeading, void());

    MOCK_METHOD1(OnStartFollowing, void(TEpochContextPtr epochContext));
    MOCK_METHOD0(OnStopFollowing, void());

    MOCK_METHOD0(GetPriority, TPeerPriority());
    MOCK_METHOD1(FormatPriority, TString(TPeerPriority priority));
};

////////////////////////////////////////////////////////////////////////////////

class TElectionManagerMock
    : public IElectionManager
{
public:
    MOCK_METHOD0(Initialize, void());
    MOCK_METHOD0(Finalize, void());

    MOCK_METHOD0(Participate, void());
    MOCK_METHOD0(Abandon, void());
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
