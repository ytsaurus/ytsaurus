#include "bigrt.h"

#include "bigrt_execution_context.h"
#include "bigrt_executor.h"
#include "composite_bigrt_writer.h"
#include "execution_block.h"
#include "pipeline_executor.h"
#include "parse_graph.h"
#include "table_poller.h"
#include "writers.h"

#include "stateful_impl/state_manager_registry.h"

#include <yt/cpp/roren/interface/executor.h>
#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/bigrt/graph/parser.h>
#include <yt/cpp/roren/bigrt/proto/config.pb.h>

#include <bigrt/lib/processing/shard_processor/stateless/processor.h>
#include <bigrt/lib/utility/profiling/safe_stats_over_yt.h>

#include <library/cpp/logger/global/global.h>

#include <util/system/info.h>

namespace NRoren {

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TBigRtDummyExecutor
    : public IExecutor
{
public:
    TBigRtDummyExecutor()
    { }

    void Run(const TPipeline&) override
    {
        Y_FAIL("Resharder cannot be run for now");
    }
};

////////////////////////////////////////////////////////////////////////////////

TTypeTag<std::function<IRawOutputPtr(TBigRtMemoryStorage&)>> MemoryOutputFactoryTag{"resharder_memory_output_factory"};
TTypeTag<TCreateBaseStateManagerFunction> CreateBaseStateManagerFunctionTag{"create_base_state_manager_function"};
TTypeTag<TBigRtStateManagerVtable> BigRtStateManagerVtableTag{"bigrt_state_manager_vtable"};
TTypeTag<TBigRtStateConfig> BigRtStateConfigTag{"bigrt_state_config"};

////////////////////////////////////////////////////////////////////////////////

}  // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

TPipeline MakeBigRtDummyPipeline()
{
    return MakePipeline(::MakeIntrusive<NPrivate::TBigRtDummyExecutor>());
}

TReadTransform<NBigRT::TMessageBatch> ReadMessageBatch(const TString& inputTag)
{
    auto result = DummyRead<NBigRT::TMessageBatch>();
    SetAttribute(result, NPrivate::InputTag, inputTag);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
