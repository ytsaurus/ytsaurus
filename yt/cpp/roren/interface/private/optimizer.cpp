#include "optimizer.h"

#include "merge_par_dos.h"
#include "raw_pipeline.h"

using namespace NRoren;
using namespace NRoren::NPrivate;

////////////////////////////////////////////////////////////////////////////////

TRawPipelinePtr TOptimizer::Optimize(const TRawPipelinePtr& rawPipeline)
{
    return MergeParDos(rawPipeline);
}
