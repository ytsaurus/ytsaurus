#include "session.h"

namespace NYT::NZookeeper {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSession
    : public ISession
{
public:
    explicit TSession(const TReqStartSessionPtr& req, TLease lease)
        : SessionId_(req->SessionId)
        , Timeout_(req->Timeout)
        , Lease_(std::move(lease))
    { }

    TSessionId GetId() const override
    {
        return SessionId_;
    }

    TDuration GetTimeout() const override
    {
        return Timeout_;
    }

    const TLease& GetLease() const override
    {
        return Lease_;
    }

private:
    const TSessionId SessionId_;
    const TDuration Timeout_;

    TLease Lease_;
};

////////////////////////////////////////////////////////////////////////////////

ISessionPtr CreateSession(const TReqStartSessionPtr& req, TLease lease)
{
    return New<TSession>(req, std::move(lease));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
