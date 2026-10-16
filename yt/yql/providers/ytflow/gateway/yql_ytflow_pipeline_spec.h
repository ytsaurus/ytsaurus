#pragma once

#include "yql_ytflow_prepare.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_user_data.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <yt/yt/flow/library/cpp/common/public.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <utility>


namespace NYql::NYtflow {

struct TRequestedCredentials
{
    TString YdbToken;
    TString MoniumToken;
};

enum class EFileDisposition
{
    InlineData,
    Path,
};

struct TFile
{
    TString Name;
    TString Content;
    EFileDisposition Disposition;
};

struct TBuildPipelineSpecContext: public NPrepare::TContext
{
    using TStreamKey = std::pair<const TExprNode*, ui32>;

    THashMap<TStringBuf, ui32>& ComputationCounters;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    const TUserDataTable& UserDataBlocks;
    THashMap<TString, TString> SecureParams;
    bool EnableComputationPatternResources;
    TVector<TFile> Files;
    THashMap<TString, ui32> StreamCounters;
    // Intermediate streams use (operation, output index); persistent streams use (source/sink, 0).
    THashMap<TStreamKey, TString> StreamNames;

public:
    TBuildPipelineSpecContext(
        NPrepare::TContext& prepareCtx,
        THashMap<TStringBuf, ui32>& computationCounters,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TUserDataTable& userDataBlocks,
        const THashMap<TString, TString>& secureParams);
};

struct TBuildPipelineSpecResult
{
    NYT::NFlow::TPipelineSpecPtr PipelineSpec;
    TRequestedCredentials RequestedCredentials;
    TVector<TFile> Files;
};

TBuildPipelineSpecResult BuildPipelineSpec(
    TExprNode::TPtr node, TBuildPipelineSpecContext& ctx);

namespace NPrivate {

TVector<TString> BuildPipelineUdfPaths(const TUserDataTable& userDataBlocks);

const TString& GetStreamName(
    const TExprNode& node, TBuildPipelineSpecContext& ctx);

void AddComputationPatternResource(
    const TString& computationName,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    TBuildPipelineSpecContext& ctx);

void AddHoppingComputationPatternResources(
    const TString& computationName,
    NYT::NFlow::TComputationSpecPtr computationSpec,
    NYT::NFlow::TPipelineSpecPtr pipelineSpec,
    TBuildPipelineSpecContext& ctx);

} // namespace NPrivate

} // namespace NYql::NYtflow
