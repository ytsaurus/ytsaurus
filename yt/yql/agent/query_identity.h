#pragma once

#include "public.h"

#include <util/generic/noncopyable.h>
#include <util/generic/string.h>

#include <string>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueryIdentityAuthority
    : public TNonCopyable
{
public:
    TQueryIdentityAuthority();

    TString IssueToken(TExecutionId executionId) const;
    TExecutionId ValidateToken(const TString& serializedToken) const;

private:
    const std::string Secret_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
