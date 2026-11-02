#pragma once

#include "private.h"

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/cpp/mapreduce/interface/fwd.h>

namespace NYT::NSequoiaReconstructor {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFileOutputConfig);

struct TFileOutputConfig
    : public NYTree::TYsonStruct
{
    std::string FileName;
    bool TextYsonOutputFormat;

    REGISTER_YSON_STRUCT(TFileOutputConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileOutputConfig);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSingleCellSequoiaReconstructorConfig);

struct TSingleCellSequoiaReconstructorConfig
    : public NYTree::TYsonStruct
{
    std::string MasterConfigPath;
    std::string SnapshotPath;

    THashMap<EOutputTableType, TFileOutputConfigPtr> TablesOutput;

    REGISTER_YSON_STRUCT(TSingleCellSequoiaReconstructorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSingleCellSequoiaReconstructorConfig);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCellConfig);

struct TCellConfig
    : public NYTree::TYsonStruct
{
    NObjectClient::TCellTag CellTag;

    TYPath SnapshotYPath;
    TYPath ConfigYPath;

    REGISTER_YSON_STRUCT(TCellConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellConfig);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TMulticellMapReduceSequoiaReconstructorConfig);

struct TMulticellMapReduceSequoiaReconstructorConfig
    : public NYTree::TYsonStruct
{
    std::string ClusterName;
    NYTree::INodePtr RpcConnectionConfig;
    std::optional<std::string> Pool;

    std::vector<TCellConfigPtr> Cells;

    TYPath WorkdirYPath;
    THashMap<EOutputTableType, TYPath> TablePaths;

    int MapperJobCount;
    double MapperCpuLimit;
    TDuration MapperTimeLimit;
    double MapperMemoryLimitGb;
    int MaxFailedJobCount;

    REGISTER_YSON_STRUCT(TMulticellMapReduceSequoiaReconstructorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMulticellMapReduceSequoiaReconstructorConfig);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TYtMapJobConfig);

struct TYtMapJobConfig
    : public NYTree::TYsonStruct
{
    std::string MasterConfigFile;
    std::string SnapshotFile;

    int JobCount;
    int JobIndex;

    THashMap<EOutputTableType, int> TableIndexes;

    REGISTER_YSON_STRUCT(TYtMapJobConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYtMapJobConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaReconstructor
