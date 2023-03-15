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
};

IExecutionContextPtr DummyExecutionContext()
{
    return MakeIntrusive<TDummyExecutionContext>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
