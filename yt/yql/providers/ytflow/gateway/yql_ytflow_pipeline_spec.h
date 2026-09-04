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
    THashMap<TStringBuf, ui32>& ComputationCounters;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    const TUserDataTable& UserDataBlocks;
    THashMap<TString, TString> SecureParams;
    bool EnableComputationPatternResources;
    TVector<TFile> Files;

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
