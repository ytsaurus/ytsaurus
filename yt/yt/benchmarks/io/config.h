#pragma once

#include "iotest.h"
#include "throttler.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDriverConfig)
DECLARE_REFCOUNTED_CLASS(TEpochConfig)
DECLARE_REFCOUNTED_CLASS(TTestConfig)

////////////////////////////////////////////////////////////////////////////////

class TDriverConfig
    : public NYTree::TYsonSerializable
{
public:
    EDriverType Type;
    NYTree::INodePtr Config;

    TDriverConfig()
    {
        RegisterParameter("type", Type)
            .Default(EDriverType::Rw);
        RegisterParameter("config", Config)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TDriverConfig)

////////////////////////////////////////////////////////////////////////////////

class TEpochConfig
    : public NYTree::TYsonSerializable
{
public:
    TString EpochName;
    std::vector<int> IterateThreads;
    std::vector<int> IterateBlockSizeLog;
    std::vector<std::pair<int, int>> IterateZone;
    std::vector<int> IterateReadPercentage;
    std::vector<bool> IterateDirect;
    std::vector<ESyncMode> IterateSync;
    std::vector<EFallocateMode> IterateFallocate;
    std::vector<bool> IterateOneshot;
    std::vector<int> IterateLoop;
    std::vector<EPattern> IteratePattern;
    std::vector<TDriverConfigPtr> IterateDriver;
    std::vector<TCombinedThrottlerConfig> IterateThrottler;
    std::vector<std::optional<i64>> IterateShotCount;
    std::vector<TString> Files;
    i64 FileSize;
    std::optional<TDuration> TimeLimit;
    std::optional<i64> TransferLimit;

    TEpochConfig()
    {
        RegisterParameter("epoch_name", EpochName);
        RegisterParameter("iterate_threads", IterateThreads)
            .Default(std::vector<int>{1});
        RegisterParameter("iterate_block_size_log", IterateBlockSizeLog)
            .Default(std::vector<int>{12});
        RegisterParameter("iterate_zone", IterateZone)
            .Default(std::vector<std::pair<int,int>>{std::pair(0,100)});
        RegisterParameter("iterate_read_percentage", IterateReadPercentage)
            .Default(std::vector<int>{100});
        RegisterParameter("iterate_direct", IterateDirect)
            .Default(std::vector<bool>{true});
        RegisterParameter("iterate_sync", IterateSync)
            .Default(std::vector<ESyncMode>{ESyncMode::None});
        RegisterParameter("iterate_fallocate", IterateFallocate)
            .Default(std::vector<EFallocateMode>{EFallocateMode::None});
        RegisterParameter("iterate_oneshot", IterateOneshot)
            .Default(std::vector<bool>{true});
        RegisterParameter("iterate_loop", IterateLoop)
            .Default(std::vector<int>{1});
        RegisterParameter("iterate_pattern", IteratePattern)
            .Default(std::vector<EPattern>{EPattern::Sequential});
        RegisterParameter("iterate_driver", IterateDriver)
            .Default(std::vector<TDriverConfigPtr>{New<TDriverConfig>()});
        RegisterParameter("iterate_throttler", IterateThrottler)
            .Default({});
        RegisterParameter("iterate_shot_count", IterateShotCount)
            .Default({});
        RegisterParameter("files", Files)
            .Optional();
        RegisterParameter("file_size", FileSize)
            .Default(1_MB);
        RegisterParameter("time_limit", TimeLimit)
            .Optional();
        RegisterParameter("transfer_limit", TransferLimit)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TEpochConfig)

////////////////////////////////////////////////////////////////////////////////

class TTestConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Name;
    std::vector<TEpochConfigPtr> Epochs;

    TTestConfig()
    {
        RegisterParameter("name", Name)
            .Optional();
        RegisterParameter("epochs", Epochs)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TTestConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
