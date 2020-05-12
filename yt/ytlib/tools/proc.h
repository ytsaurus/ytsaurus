#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

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

class TMountTmpfsConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Path;
    int UserId;
    i64 Size;

    TMountTmpfsConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("user_id", UserId)
            .GreaterThanOrEqual(0);
        RegisterParameter("size", Size)
            .GreaterThanOrEqual(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TMountTmpfsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMountTmpfsAsRootTool
{
    void operator()(TMountTmpfsConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

class TUmountConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Path;
    bool Detach;

    TUmountConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("detach", Detach);
    }
};

DEFINE_REFCOUNTED_TYPE(TUmountConfig)

////////////////////////////////////////////////////////////////////////////////

struct TUmountAsRootTool
{
    void operator()(TUmountConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSetThreadPriorityConfig
    : public NYTree::TYsonSerializable
{
public:
    int ThreadId;
    int Priority;

    TSetThreadPriorityConfig()
    {
        RegisterParameter("thread_id", ThreadId);
        RegisterParameter("priority", Priority);
    }
};

DEFINE_REFCOUNTED_TYPE(TSetThreadPriorityConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSetThreadPriorityAsRootTool
{
    void operator()(TSetThreadPriorityConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

class TFSQuotaConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<i64> DiskSpaceLimit;
    std::optional<i64> InodeLimit;
    int UserId;
    TString Path;

    TFSQuotaConfig()
    {
        RegisterParameter("disk_space_limit", DiskSpaceLimit)
            .GreaterThanOrEqual(0)
            .Default();
        RegisterParameter("inode_limit", InodeLimit)
            .GreaterThanOrEqual(0)
            .Default();
        RegisterParameter("user_id", UserId)
            .GreaterThanOrEqual(0);
        RegisterParameter("path", Path);
    }
};

DEFINE_REFCOUNTED_TYPE(TFSQuotaConfig)

struct TFSQuotaTool
{
    void operator()(TFSQuotaConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

class TChownChmodConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Path;
    std::optional<uid_t> UserId;
    std::optional<int> Permissions;

    TChownChmodConfig()
    {
        RegisterParameter("path", Path)
            .NonEmpty();
        RegisterParameter("user_id", UserId)
            .Default();
        RegisterParameter("permissions", Permissions)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TChownChmodConfig)

struct TChownChmodTool
{
    void operator()(TChownChmodConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TGetDirectorySizeAsRootTool
{
    i64 operator()(const TString& path) const;
};

////////////////////////////////////////////////////////////////////////////////

class TCopyDirectoryContentConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Source;
    TString Destination;

    TCopyDirectoryContentConfig()
    {
        RegisterParameter("source", Source)
            .NonEmpty();
        RegisterParameter("destination", Destination)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TCopyDirectoryContentConfig)

struct TCopyDirectoryContentTool
{
    void operator()(TCopyDirectoryContentConfigPtr config) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools
