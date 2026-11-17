#include <yt/yql/providers/dq/runtime/task_command_executor.h>

#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>

#include <util/stream/file.h>

#include <util/system/env.h>

#include <unistd.h>

namespace {

class TBlockingNode
    : public NKikimr::NMiniKQL::TMutableComputationNode<TBlockingNode>
{
public:
    explicit TBlockingNode(NKikimr::NMiniKQL::TComputationMutables& mutables)
        : TMutableComputationNode(mutables)
    { }

    NYql::NUdf::TUnboxedValuePod DoCalculate(
        NKikimr::NMiniKQL::TComputationContext& /*ctx*/) const
    {
        {
            TFileOutput output(GetEnv("DQ_PIPE_SMOKE_RUNNING_FILE"));
            output << "running";
        }
        while (true) {
            pause();
        }
    }

private:
    void RegisterDependencies() const final
    { }
};

} // namespace

int main()
{
    {
        TFileOutput output(GetEnv("DQ_PIPE_SMOKE_PID_FILE"));
        output << getpid();
    }

    return NYql::NTaskRunnerProxy::CreateTaskCommandExecutor(
        NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
            [] (NKikimr::NMiniKQL::TCallable& callable,
                const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx)
                -> NKikimr::NMiniKQL::IComputationNode*
            {
                if (callable.GetType()->GetName() == "PipeSmokeBlock") {
                    return new TBlockingNode(ctx.Mutables);
                }
                return nullptr;
            },
        }),
        [] (const NYql::TTaskTransformArguments&, const NKikimr::NMiniKQL::IFunctionRegistry*) {
            return NKikimr::NMiniKQL::TCallableVisitFuncProvider{};
        },
        /*jobStats*/ nullptr);
}
