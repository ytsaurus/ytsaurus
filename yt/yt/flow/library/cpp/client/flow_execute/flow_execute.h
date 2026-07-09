#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/client/api/public.h>

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

} // namespace NYT::NFlow
