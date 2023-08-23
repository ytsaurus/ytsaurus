#include "dummy_pipeline.h"

#include "../executor.h"
#include "../roren.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TDummyExecutor
    : public IExecutor
{
public:
    void Run(const TPipeline&) override
    {
        Y_FAIL("TDummyExecutor is not meant to execute pipelines");
    }
};

TPipeline MakeDummyPipeline()
{
    return MakePipeline(MakeIntrusive<TDummyExecutor>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
