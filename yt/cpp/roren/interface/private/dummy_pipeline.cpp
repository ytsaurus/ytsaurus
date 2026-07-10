#include "dummy_pipeline.h"

#include <yt/cpp/roren/interface/executor.h>
#include <yt/cpp/roren/interface/roren.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TDummyExecutor
    : public IExecutor
{
public:
    void Run(const TPipeline&) override
    {
        Y_ABORT("TDummyExecutor is not meant to execute pipelines");
    }
};

TPipeline MakeDummyPipeline()
{
    return MakePipeline(NYT::New<TDummyExecutor>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
