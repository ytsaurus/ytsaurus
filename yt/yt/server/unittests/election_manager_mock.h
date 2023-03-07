#pragma once

#include <yt/core/test_framework/framework.h>

#include <yt/server/lib/election/election_manager.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionCallbacksMock
    : public IElectionCallbacks
{
public:
    MOCK_METHOD1(OnStartLeading, void(TEpochContextPtr epochContext));
    MOCK_METHOD1(OnStopLeading, void(const TError&));

    MOCK_METHOD1(OnStartFollowing, void(TEpochContextPtr epochContext));
    MOCK_METHOD1(OnStopFollowing, void(const TError&));

    MOCK_METHOD1(OnStopVoting, void(const TError&));

    MOCK_METHOD0(GetPriority, TPeerPriority());
    MOCK_METHOD1(FormatPriority, TString(TPeerPriority priority));

    MOCK_METHOD1(OnAlivePeerSetChanged, void(const TPeerIdSet& alivePeers));
};

////////////////////////////////////////////////////////////////////////////////

class TElectionManagerMock
    : public IElectionManager
{
public:
    MOCK_METHOD0(Initialize, void());
    MOCK_METHOD0(Finalize, void());

    MOCK_METHOD0(Participate, void());
    MOCK_METHOD1(Abandon, void(const TError&));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
