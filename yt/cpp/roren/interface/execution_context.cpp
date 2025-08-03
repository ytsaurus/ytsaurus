#include "execution_context.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

const NYT::NProfiling::TSensorsOwner& IExecutionContext::GetSensorsOwner() const
{
    return NYT::NProfiling::GetRootSensorsOwner();
}

const NYT::NLogging::TLogger& IExecutionContext::GetLogger() const
{
    static const NYT::NLogging::TLogger Logger{"Roren"};
    return Logger;
}

TInstant IExecutionContext::GetTime() const
{
    return NYT::GetInstant();
}

////////////////////////////////////////////////////////////////////////////////

class TDummyExecutionContext
    : public IExecutionContext
{
public:
    TString GetExecutorName() const override
    {
        return "unknown";
    }

    NYT::NProfiling::TProfiler GetProfiler() const override
    {
        return {};
    }

    void SetTimer(const TTimer& timer, const TTimer::EMergePolicy policy) override
    {
        Y_UNUSED(timer);
        Y_UNUSED(policy);
        Y_ABORT("not implemented");
    }

    void DeleteTimer(const TTimer::TKey& key) override
    {
        Y_UNUSED(key);
        Y_ABORT("not implemented");
    }
};

IExecutionContextPtr DummyExecutionContext()
{
    return NYT::New<TDummyExecutionContext>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
