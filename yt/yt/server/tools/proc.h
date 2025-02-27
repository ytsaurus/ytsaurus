#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

struct TRemoveDirAsRootTool
{
    void operator()(const TString& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TKillAllByUidTool
{
    void operator()(int uid) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TRemoveDirContentAsRootTool
{
    void operator()(const TString& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateDirectoryAsRootTool
{
    void operator()(const TString& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TMountTmpfsConfig
    : public NYTree::TYsonStruct
{
    TString Path;
    int UserId;
    i64 Size;

    REGISTER_YSON_STRUCT(TMountTmpfsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMountTmpfsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMountTmpfsAsRootTool
{
    void operator()(TMountTmpfsConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TSpawnShellConfig
    : public NYTree::TYsonStruct
{
    std::optional<TString> Command;

    REGISTER_YSON_STRUCT(TSpawnShellConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSpawnShellConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSpawnShellTool
{
    void operator()(TSpawnShellConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TUmountConfig
    : public NYTree::TYsonStruct
{
    TString Path;
    bool Detach;

    REGISTER_YSON_STRUCT(TUmountConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUmountConfig)

////////////////////////////////////////////////////////////////////////////////

struct TUmountAsRootTool
{
    void operator()(TUmountConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TSetThreadPriorityConfig
    : public NYTree::TYsonStruct
{
    int ThreadId;
    int Priority;

    REGISTER_YSON_STRUCT(TSetThreadPriorityConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSetThreadPriorityConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSetThreadPriorityAsRootTool
{
    void operator()(TSetThreadPriorityConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TFSQuotaConfig
    : public NYTree::TYsonStruct
{
    std::optional<i64> DiskSpaceLimit;
    std::optional<i64> InodeLimit;
    int UserId;
    TString Path;

    REGISTER_YSON_STRUCT(TFSQuotaConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFSQuotaConfig)

struct TFSQuotaTool
{
    void operator()(TFSQuotaConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TChownChmodConfig
    : public NYTree::TYsonStruct
{
    TString Path;
    std::optional<uid_t> UserId;
    std::optional<int> Permissions;

    REGISTER_YSON_STRUCT(TChownChmodConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChownChmodConfig)

struct TChownChmodTool
{
    void operator()(TChownChmodConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TGetDirectorySizesAsRootConfig
    : public NYTree::TYsonStruct
{
    std::vector<TString> Paths;
    bool IgnoreUnavailableFiles;
    bool DeduplicateByINodes;
    bool CheckDeviceId;

    REGISTER_YSON_STRUCT(TGetDirectorySizesAsRootConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGetDirectorySizesAsRootConfig)

struct TGetDirectorySizesAsRootTool
{
    std::vector<i64> operator()(const TGetDirectorySizesAsRootConfigPtr& config) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TCopyDirectoryContentConfig
    : public NYTree::TYsonStruct
{
    TString Source;
    TString Destination;

    REGISTER_YSON_STRUCT(TCopyDirectoryContentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCopyDirectoryContentConfig)

struct TCopyDirectoryContentTool
{
    void operator()(TCopyDirectoryContentConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TReadProcessSmapsTool
{
    TString operator()(int pid) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TDirectoryConfig
    : public NYTree::TYsonStruct
{
    TString Path;
    std::optional<int> UserId;
    std::optional<int> Permissions;
    bool RemoveIfExists;

    REGISTER_YSON_STRUCT(TDirectoryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

struct TRootDirectoryConfig
    : public NYTree::TYsonStruct
{
    TString SlotPath;
    std::optional<int> UserId;
    int Permissions;

    std::vector<TDirectoryConfigPtr> Directories;

    REGISTER_YSON_STRUCT(TRootDirectoryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRootDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDirectoryBuilderConfig
    : public NYTree::TYsonStruct
{
    int NodeUid;

    bool NeedRoot;

    std::vector<TRootDirectoryConfigPtr> RootDirectoryConfigs;

    REGISTER_YSON_STRUCT(TDirectoryBuilderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDirectoryBuilderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TRootDirectoryBuilderTool
{
    void operator()(const TDirectoryBuilderConfigPtr& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools
