#include "yql_ytflow_common_parameters.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYql::NYtflow {

void TCommonOperationParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("udf_paths", &TThis::UdfPaths);
    registrar.Parameter("output_indices_by_output_stream_id", &TThis::OutputIndicesByOutputStreamId);
    registrar.Parameter("lang_version", &TThis::LangVersion);
    registrar.Parameter("opt_llvm", &TThis::OptLLVM)
        .Default(TString("OFF"));
    registrar.Parameter("runtime_settings", &TThis::RuntimeSettings);
}

void TCommonMapParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("lambda_file", &TThis::LambdaFile);
    registrar.Parameter("inject_input_message_id", &TThis::InjectInputMessageId)
        .Default(false);
}

void ValidateMapSpec(const NYT::NFlow::TComputationSpec& spec)
{
    THROW_ERROR_EXCEPTION_IF(
        spec.InputStreamIds.size() > 1 &&
            !spec.Parameters->GetChildValueOrDefault<bool>("extend", false),
        "Computation %Qv supports multiple input streams only in Extend mode",
        spec.ComputationClassName);
}

} // namespace NYql::NYtflow
