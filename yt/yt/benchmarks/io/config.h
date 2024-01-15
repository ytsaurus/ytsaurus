#pragma once

#include "iotest.h"
#include "throttler.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDriverConfig)
DECLARE_REFCOUNTED_CLASS(TEpochConfig)
DECLARE_REFCOUNTED_CLASS(TTestConfig)

////////////////////////////////////////////////////////////////////////////////

class TDriverConfig
    : public NYTree::TYsonStruct
{
public:
    EDriverType Type;
    NYTree::INodePtr Config;

    REGISTER_YSON_STRUCT(TDriverConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("type", &TThis::Type)
            .Default(EDriverType::Rw);
        registrar.Parameter("config", &TThis::Config)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TDriverConfig)

////////////////////////////////////////////////////////////////////////////////

class TEpochConfig
    : public NYTree::TYsonStruct
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

    REGISTER_YSON_STRUCT(TEpochConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("epoch_name", &TThis::EpochName);
        registrar.Parameter("iterate_threads", &TThis::IterateThreads)
            .Default(std::vector<int>{1});
        registrar.Parameter("iterate_block_size_log", &TThis::IterateBlockSizeLog)
            .Default(std::vector<int>{12});
        registrar.Parameter("iterate_zone", &TThis::IterateZone)
            .Default(std::vector<std::pair<int,int>>{std::pair(0,100)});
        registrar.Parameter("iterate_read_percentage", &TThis::IterateReadPercentage)
            .Default(std::vector<int>{100});
        registrar.Parameter("iterate_direct", &TThis::IterateDirect)
            .Default(std::vector<bool>{true});
        registrar.Parameter("iterate_sync", &TThis::IterateSync)
            .Default(std::vector<ESyncMode>{ESyncMode::None});
        registrar.Parameter("iterate_fallocate", &TThis::IterateFallocate)
            .Default(std::vector<EFallocateMode>{EFallocateMode::None});
        registrar.Parameter("iterate_oneshot", &TThis::IterateOneshot)
            .Default(std::vector<bool>{true});
        registrar.Parameter("iterate_loop", &TThis::IterateLoop)
            .Default(std::vector<int>{1});
        registrar.Parameter("iterate_pattern", &TThis::IteratePattern)
            .Default(std::vector<EPattern>{EPattern::Sequential});
        registrar.Parameter("iterate_driver", &TThis::IterateDriver)
            .Default(std::vector<TDriverConfigPtr>{New<TDriverConfig>()});
        registrar.Parameter("iterate_throttler", &TThis::IterateThrottler)
            .Default({});
        registrar.Parameter("iterate_shot_count", &TThis::IterateShotCount)
            .Default({});
        registrar.Parameter("files", &TThis::Files)
            .Optional();
        registrar.Parameter("file_size", &TThis::FileSize)
            .Default(1_MB);
        registrar.Parameter("time_limit", &TThis::TimeLimit)
            .Optional();
        registrar.Parameter("transfer_limit", &TThis::TransferLimit)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TEpochConfig)

////////////////////////////////////////////////////////////////////////////////

class TTestConfig
    : public NYTree::TYsonStruct
{
public:
    TString Name;
    std::vector<TEpochConfigPtr> Epochs;

    REGISTER_YSON_STRUCT(TTestConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("name", &TThis::Name)
            .Optional();
        registrar.Parameter("epochs", &TThis::Epochs)
            .Optional();
    }
};

DEFINE_REFCOUNTED_TYPE(TTestConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
