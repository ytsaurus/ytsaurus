#pragma once

#include "election_manager.h"

#include <contrib/testing/gmock.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionCallbacksMock
    : public IElectionCallbacks
{
public:
    MOCK_METHOD0(OnStartLeading, void());
    MOCK_METHOD0(OnStopLeading, void());
    MOCK_METHOD0(OnStartFollowing, void());
    MOCK_METHOD0(OnStopFollowing, void());

    MOCK_METHOD0(GetPriority, TPeerPriority());
    MOCK_METHOD1(FormatPriority, Stroka(TPeerPriority priority));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
