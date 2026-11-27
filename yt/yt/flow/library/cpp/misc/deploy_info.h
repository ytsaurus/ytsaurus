#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/misc/enum.h>

#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TYDeployInfo)
DECLARE_REFCOUNTED_STRUCT(TVanillaDeployInfo)
DECLARE_REFCOUNTED_STRUCT(TDeployInfo)

// Spelled "Ydeploy" so that it is serialized as "ydeploy".
DEFINE_ENUM(EDeployType,
    (Ydeploy)
    (Vanilla)
);

////////////////////////////////////////////////////////////////////////////////

struct TYDeployInfo
    : public NYTree::TYsonStruct
{
    std::string StageId;
    //! Link to the stage UI page; empty when unknown.
    std::string StageUrl;

    REGISTER_YSON_STRUCT(TYDeployInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYDeployInfo)

////////////////////////////////////////////////////////////////////////////////

struct TVanillaDeployInfo
    : public NYTree::TYsonStruct
{
    std::string Cluster;
    std::string OperationId;
    std::string JobId;

    REGISTER_YSON_STRUCT(TVanillaDeployInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVanillaDeployInfo)

////////////////////////////////////////////////////////////////////////////////

//! Describes how the current process is deployed.
//! Exactly the sub-struct matching #Type is set.
struct TDeployInfo
    : public NYTree::TYsonStruct
{
    EDeployType Type{};
    TYDeployInfoPtr YDeploy;
    TVanillaDeployInfoPtr Vanilla;

    REGISTER_YSON_STRUCT(TDeployInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDeployInfo)

////////////////////////////////////////////////////////////////////////////////

//! Builds the deploy info from the process environment.
//! Returns null when the deployment environment is not recognized.
TDeployInfoPtr MakeDeployInfo();

//! Cached #MakeDeployInfo result.
const TDeployInfoPtr& GetDeployInfo();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
