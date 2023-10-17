#include "execution_context.h"

namespace NRoren {

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
    return MakeIntrusive<TDummyExecutionContext>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
