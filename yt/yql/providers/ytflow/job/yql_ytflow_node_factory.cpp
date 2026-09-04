#include "yql_ytflow_node_factory.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/public/udf/udf_type_builder.h>

#include <yt/yql/providers/ytflow/comp_nodes/mkql_ytflow_chunked_forward_list.h>
#include <yt/yql/providers/ytflow/comp_nodes/mkql_ytflow_lookup_join.h>

#include <util/generic/string.h>
#include <util/stream/file.h>


namespace NYql::NYtflow {
namespace {

template <EYtflowCallablePatternSharing Sharing>
struct TCallableDescriptor {
    static constexpr auto PatternSharing = Sharing;
};

struct TYtflowInputStreamCallable
    : TCallableDescriptor<EYtflowCallablePatternSharing::Shareable> {
    static constexpr TStringBuf Name = "YtflowInputStream";
};

struct TYtflowInputStateCallable
    : TCallableDescriptor<EYtflowCallablePatternSharing::Shareable> {
    static constexpr TStringBuf Name = "YtflowInputState";
};

struct TYtflowInputKeyCallable
    : TCallableDescriptor<EYtflowCallablePatternSharing::Shareable> {
    static constexpr TStringBuf Name = "YtflowInputKey";
};

struct TYtflowInputMaxHopStartTimeCallable
    : TCallableDescriptor<EYtflowCallablePatternSharing::Shareable> {
    static constexpr TStringBuf Name = "YtflowInputMaxHopStartTime";
};

struct TFilePathCallable
    : TCallableDescriptor<EYtflowCallablePatternSharing::Shareable> {
    static constexpr TStringBuf Name = "FilePath";
};

struct TFileContentCallable
    : TCallableDescriptor<EYtflowCallablePatternSharing::Shareable> {
    static constexpr TStringBuf Name = "FileContent";
};

struct TYtflowLookupJoinCallable
    : TCallableDescriptor<EYtflowCallablePatternSharing::Shareable> {
    static constexpr TStringBuf Name = "YtflowLookupJoin";
};

struct TYtflowChunkedForwardListCallable
    : TCallableDescriptor<EYtflowCallablePatternSharing::Shareable> {
    static constexpr TStringBuf Name = "YtflowChunkedForwardList";
};

template <class TCallable>
bool MatchCallable(
    TStringBuf callableName,
    EYtflowCallablePatternSharing& sharing)
{
    if (callableName != TCallable::Name) {
        return false;
    }

    sharing = TCallable::PatternSharing;
    return true;
}

bool MatchYtflowInputCallable(
    TStringBuf callableName,
    EYtflowCallablePatternSharing& sharing)
{
    return MatchCallable<TYtflowInputStreamCallable>(callableName, sharing) ||
        MatchCallable<TYtflowInputStateCallable>(callableName, sharing) ||
        MatchCallable<TYtflowInputKeyCallable>(callableName, sharing) ||
        MatchCallable<TYtflowInputMaxHopStartTimeCallable>(callableName, sharing);
}

} // namespace

NKikimr::NMiniKQL::TComputationNodeFactory GetNodeFactory(
    const TNodeFactoryMetadata& metadata,
    TNodeFactoryResult& result
) {
    return [&metadata, &result](
        NKikimr::NMiniKQL::TCallable& callable,
        const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx
    ) -> NKikimr::NMiniKQL::IComputationNode*
    {
        auto callableName = callable.GetType()->GetName();
        auto sharing = EYtflowCallablePatternSharing::Unknown;
        NKikimr::NMiniKQL::IComputationNode* node = nullptr;

        if (MatchYtflowInputCallable(callableName, sharing)) {
            MKQL_ENSURE(
                callable.GetInputsCount() == 0,
                "Unexpected inputs count: " << callable.GetInputsCount());

            auto* inputNode =
                new NKikimr::NMiniKQL::TExternalComputationNode(ctx.Mutables);

            auto callableNameKey = TString(callableName);
            result.InputTypes[callableNameKey] =
                callable.GetType()->GetReturnType();

            result.YtflowInputNodes[callableNameKey] = inputNode;

            node = inputNode;
        } else if (MatchCallable<TFilePathCallable>(callableName, sharing) ||
                   MatchCallable<TFileContentCallable>(callableName, sharing)) {
            // TODO(ngc224): make indirection
            auto fileName = TString(
                AS_VALUE(NKikimr::NMiniKQL::TDataLiteral, callable.GetInput(0))
                    ->AsValue().AsStringRef());

            auto content = callableName == TFileContentCallable::Name
                ? TFileInput(fileName).ReadAll()
                : fileName;

            node = ctx.NodeFactory.CreateImmutableNode(
                NKikimr::NMiniKQL::MakeString(content));
        } else if (MatchCallable<TYtflowLookupJoinCallable>(callableName, sharing)) {
            node = WrapYtflowLookupJoin(
                callable,
                ctx,
                metadata.YtflowLookupProviderRegistry);
        } else if (MatchCallable<TYtflowChunkedForwardListCallable>(callableName, sharing)) {
            node = WrapYtflowChunkedForwardList(
                callable,
                ctx);
        }

        if (node) {
            result.PatternMetadata.SpecializedCallables.emplace(
                callableName,
                sharing);
        }

        return node;
    };
}

} // namespace NYql::NYtflow
