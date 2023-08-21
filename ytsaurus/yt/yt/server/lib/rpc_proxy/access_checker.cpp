#include "access_checker.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TNoopAccessChecker
    : public IAccessChecker
{
public:
    TError CheckAccess(const TString& /*user*/) const override
    {
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

IAccessCheckerPtr CreateNoopAccessChecker()
{
    return New<TNoopAccessChecker>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
