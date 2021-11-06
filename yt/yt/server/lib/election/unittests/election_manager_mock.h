#pragma once

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/election/election_manager.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionCallbacksMock
    : public IElectionCallbacks
{
public:
    MOCK_METHOD(void, OnStartLeading, (TEpochContextPtr epochContext), (override));
    MOCK_METHOD(void, OnStopLeading, (const TError&), (override));

    MOCK_METHOD(void, OnStartFollowing, (TEpochContextPtr epochContext), (override));
    MOCK_METHOD(void, OnStopFollowing, (const TError&), (override));

    MOCK_METHOD(void, OnStopVoting, (const TError&), (override));

    MOCK_METHOD(TPeerPriority, GetPriority, (), (override));
    MOCK_METHOD(TString, FormatPriority, (TPeerPriority priority), (override));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
