#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/client/api/flow_client.h>
#include <yt/yt/client/api/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TGetFlowViewArg
    : public NYTree::TYsonStructLite
{
    std::string Path;
    bool Cache{};

    REGISTER_YSON_STRUCT_LITE(TGetFlowViewArg);

    static void Register(TRegistrar registrar);
};

using TGetFlowViewResult = NYson::TYsonString;

//! Argument of the get-flow-view-v2 command (same shape as get-flow-view: |Path| + |Cache|).
struct TGetFlowViewV2Arg
    : public TGetFlowViewArg
{
    REGISTER_YSON_STRUCT_LITE(TGetFlowViewV2Arg);

    static void Register(TRegistrar registrar);
};

//! Payload returned by the get-flow-view-v2 command: the (full or sub-path) flow view YSON compressed
//! with |Codec|. Recover the plain flow view YSON via |DecompressFlowView|.
struct TGetFlowViewV2Result
    : public NYTree::TYsonStructLite
{
    NCompression::ECodec Codec{};
    std::string Data;

    REGISTER_YSON_STRUCT_LITE(TGetFlowViewV2Result);

    static void Register(TRegistrar registrar);
};

TGetFlowViewResult DecompressFlowView(const TGetFlowViewV2Result& compressed);

//! Fetches the flow view via FlowExecute. If the controller advertises "get-flow-view-v2" (checked via
//! the "list" command) it uses that compressed command and decompresses transparently; otherwise it falls
//! back to the uncompressed "get-flow-view". Returns the plain flow view YSON. Must be called from a fiber.
TGetFlowViewResult GetFlowView(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& pipelinePath,
    const TGetFlowViewArg& arg);

////////////////////////////////////////////////////////////////////////////////

struct TGetPipelineDynamicSpecArg
    : public NYTree::TYsonStructLite
{
    std::string Path;

    REGISTER_YSON_STRUCT_LITE(TGetPipelineDynamicSpecArg);

    static void Register(TRegistrar registrar);
};

struct TGetPipelineDynamicSpecResult
    : public NYTree::TYsonStructLite
{
    NYTree::INodePtr Spec;
    TVersion Version;

    REGISTER_YSON_STRUCT_LITE(TGetPipelineDynamicSpecResult);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TSetPipelineDynamicSpecArg
    : public NYTree::TYsonStructLite
{
    NYTree::INodePtr Spec;
    std::string Path;
    std::optional<TVersion> ExpectedVersion;

    REGISTER_YSON_STRUCT_LITE(TSetPipelineDynamicSpecArg);

    static void Register(TRegistrar registrar);
};

struct TSetPipelineDynamicSpecResult
    : public NYTree::TYsonStructLite
{
    TVersion Version;

    REGISTER_YSON_STRUCT_LITE(TSetPipelineDynamicSpecResult);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TGetPipelineSpecArg
    : public NYTree::TYsonStructLite
{
    std::string Path;

    REGISTER_YSON_STRUCT_LITE(TGetPipelineSpecArg);

    static void Register(TRegistrar registrar);
};

struct TGetPipelineSpecResult
    : public NYTree::TYsonStructLite
{
    NYTree::INodePtr Spec;
    TVersion Version;

    REGISTER_YSON_STRUCT_LITE(TGetPipelineSpecResult);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TSetPipelineSpecArg
    : public NYTree::TYsonStructLite
{
    NYTree::INodePtr Spec;
    std::optional<TVersion> ExpectedVersion;
    bool Force{};

    REGISTER_YSON_STRUCT_LITE(TSetPipelineSpecArg);

    static void Register(TRegistrar registrar);
};

struct TSetPipelineSpecResult
    : public NYTree::TYsonStructLite
{
    TVersion Version;

    REGISTER_YSON_STRUCT_LITE(TSetPipelineSpecResult);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TSetPipelineSpecsArg
    : public NYTree::TYsonStructLite
{
    std::optional<NYTree::INodePtr> Spec;
    std::optional<NYTree::INodePtr> DynamicSpec;
    std::optional<TVersion> ExpectedSpecVersion;
    std::optional<TVersion> ExpectedDynamicSpecVersion;
    bool AllowSpecUpdateOnPause{};
    bool ValidateStrict{};
    bool Force{};

    REGISTER_YSON_STRUCT_LITE(TSetPipelineSpecsArg);

    static void Register(TRegistrar registrar);
};

struct TSetPipelineSpecsResult
    : public NYTree::TYsonStructLite
{
    TVersion SpecVersion;
    TVersion DynamicSpecVersion;

    REGISTER_YSON_STRUCT_LITE(TSetPipelineSpecsResult);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TGetPipelineStateArg
    : public NYTree::TYsonStructLite
{
    REGISTER_YSON_STRUCT_LITE(TGetPipelineStateArg);

    static void Register(TRegistrar registrar);
};

struct TGetPipelineStateResult
    : public NYTree::TYsonStructLite
{
    EPipelineState PipelineState{};

    REGISTER_YSON_STRUCT_LITE(TGetPipelineStateResult);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TSetTargetPipelineStateArg
    : public NYTree::TYsonStructLite
{
    EPipelineState TargetPipelineState{};

    REGISTER_YSON_STRUCT_LITE(TSetTargetPipelineStateArg);

    static void Register(TRegistrar registrar);
};

struct TSetTargetPipelineStateResult
    : public NYTree::TYsonStructLite
{
    REGISTER_YSON_STRUCT_LITE(TSetTargetPipelineStateResult);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TGetControllerOrchidArg
    : public NYTree::TYsonStructLite
{
    std::string Path;

    REGISTER_YSON_STRUCT_LITE(TGetControllerOrchidArg);

    static void Register(TRegistrar registrar);
};

struct TGetControllerOrchidResult
    : public NYTree::TYsonStructLite
{
    NYson::TYsonString Value;

    REGISTER_YSON_STRUCT_LITE(TGetControllerOrchidResult);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TGetFlowCoreTargetArg
    : public NYTree::TYsonStructLite
{
    REGISTER_YSON_STRUCT_LITE(TGetFlowCoreTargetArg);

    static void Register(TRegistrar registrar);
};

struct TGetFlowCoreTargetResult
    : public NYTree::TYsonStructLite
{
    TFlowCoreTarget FlowCoreTarget;
    TVersion Version;

    REGISTER_YSON_STRUCT_LITE(TGetFlowCoreTargetResult);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TSetFlowCoreTargetArg
    : public NYTree::TYsonStructLite
{
    TFlowCoreTarget FlowCoreTarget;
    bool AllowUpdateOnPause{};
    std::optional<TVersion> ExpectedVersion;

    REGISTER_YSON_STRUCT_LITE(TSetFlowCoreTargetArg);

    static void Register(TRegistrar registrar);
};

struct TSetFlowCoreTargetResult
    : public NYTree::TYsonStructLite
{
    TVersion Version;

    REGISTER_YSON_STRUCT_LITE(TSetFlowCoreTargetResult);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Direct mode: the commands the runner would otherwise send through the RPC proxy go to the
//! pipeline controller itself.
struct TDirectControllerCommandsConfig
    : public NYTree::TYsonStruct
{
    bool Enabled{};

    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TDirectControllerCommandsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDirectControllerCommandsConfig)

////////////////////////////////////////////////////////////////////////////////

//! Where the flow_execute commands go. Converts from a plain client, which keeps them on the
//! RPC proxy path.
struct TFlowExecuteTarget
{
    //! Client of the pipeline cluster. In the direct mode it reads the published leader and
    //! carries the credentials the controller authenticates.
    NApi::IClientPtr Client;

    //! When set and enabled, the commands bypass the RPC proxy and go to the leader controller.
    TDirectControllerCommandsConfigPtr DirectControllerCommands;

    //! Channels to the leader controller; set in the direct mode only. Polling commands reuse
    //! the channel of the published leader instead of connecting anew.
    NRpc::IChannelFactoryPtr ChannelFactory;

    TFlowExecuteTarget(NApi::IClientPtr client, TDirectControllerCommandsConfigPtr directControllerCommands = nullptr);

    bool IsDirect() const;
};

//! The command name and the result type of a flow_execute argument; specialized per command below.
template <class TArg>
struct TFlowExecuteTraits;

//! Runs |command| with a YSON argument and returns the YSON result. Must be called from a fiber.
NYson::TYsonString FlowExecute(
    const TFlowExecuteTarget& target,
    const NYPath::TYPath& pipelinePath,
    const std::string& command,
    const NYson::TYsonString& argument,
    const NApi::TFlowExecuteOptions& options = {});

//! Same, with the command name and the result type taken from |argument|. Must be called from a fiber.
template <class TArg>
typename TFlowExecuteTraits<TArg>::TResult FlowExecute(
    const TFlowExecuteTarget& target,
    const NYPath::TYPath& pipelinePath,
    const TArg& argument,
    const NApi::TFlowExecuteOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

#define YT_FLOW_DEFINE_COMMAND(argType, resultType, commandName) \
    template <>                                                  \
    struct TFlowExecuteTraits<argType>                           \
    {                                                            \
        using TResult = resultType;                              \
        static constexpr TStringBuf Command = commandName;       \
    };

YT_FLOW_DEFINE_COMMAND(TGetPipelineSpecArg, TGetPipelineSpecResult, "get-pipeline-spec")
YT_FLOW_DEFINE_COMMAND(TSetPipelineSpecArg, TSetPipelineSpecResult, "set-pipeline-spec")
YT_FLOW_DEFINE_COMMAND(TGetPipelineDynamicSpecArg, TGetPipelineDynamicSpecResult, "get-pipeline-dynamic-spec")
YT_FLOW_DEFINE_COMMAND(TSetPipelineDynamicSpecArg, TSetPipelineDynamicSpecResult, "set-pipeline-dynamic-spec")
YT_FLOW_DEFINE_COMMAND(TSetPipelineSpecsArg, TSetPipelineSpecsResult, "set-pipeline-specs")
YT_FLOW_DEFINE_COMMAND(TGetPipelineStateArg, TGetPipelineStateResult, "get-pipeline-state")
YT_FLOW_DEFINE_COMMAND(TSetTargetPipelineStateArg, TSetTargetPipelineStateResult, "set-target-pipeline-state")
YT_FLOW_DEFINE_COMMAND(TGetControllerOrchidArg, TGetControllerOrchidResult, "get-controller-orchid")
YT_FLOW_DEFINE_COMMAND(TGetFlowCoreTargetArg, TGetFlowCoreTargetResult, "get-flow-core-target")
YT_FLOW_DEFINE_COMMAND(TSetFlowCoreTargetArg, TSetFlowCoreTargetResult, "set-flow-core-target")
YT_FLOW_DEFINE_COMMAND(TGetFlowViewV2Arg, TGetFlowViewV2Result, "get-flow-view-v2")

#undef YT_FLOW_DEFINE_COMMAND

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define FLOW_EXECUTE_INL_H_
#include "flow_execute-inl.h"
#undef FLOW_EXECUTE_INL_H_
