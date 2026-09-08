#pragma once

#include <yql/essentials/public/langver/yql_langver.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <yt/yt/flow/library/cpp/computation/computation_base.h>

namespace NYql::NYtflow {

struct TCommonOperationParameters
    : public NYT::NFlow::IComputation::TParameters
{
    TVector<TString> UdfPaths;
    THashMap<NYT::NFlow::TStreamId, TString> OutputIndicesByOutputStreamId;
    TLangVersion LangVersion;
    TString OptLLVM;
    TString RuntimeSettings;

    REGISTER_YSON_STRUCT(TCommonOperationParameters);

    static void Register(TRegistrar registrar);
};

struct TCommonMapParameters
    : public TCommonOperationParameters
{
    TString LambdaFile;
    bool InjectInputMessageId;

    REGISTER_YSON_STRUCT(TCommonMapParameters);

    static void Register(TRegistrar registrar);
};

void ValidateMapSpec(const NYT::NFlow::TComputationSpec& spec);

} // namespace NYql::NYtflow
