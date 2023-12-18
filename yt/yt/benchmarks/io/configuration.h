#pragma once

#include "iotest.h"
#include "throttler.h"

#include <util/system/file.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

struct TFile
{
    ::TFile File;
    i64 Size;
};

struct TDriverDescription
{
    EDriverType Type = EDriverType::Memcpy;
    NYTree::INodePtr Config;
};

struct TConfiguration
    : public TRefCounted
{
    TInstant Start;
    TDriverDescription Driver;
    int Threads = 1;
    int BlockSizeLog = 12;
    std::pair<int, int> Zone = std::pair<int,int>(0, 100);
    EPattern Pattern = EPattern::Sequential;
    TCombinedThrottlerConfig Throttler;
    int ReadPercentage = 100;
    bool Direct = true;
    ESyncMode Sync = ESyncMode::None;
    EFallocateMode Fallocate = EFallocateMode::None;
    bool Oneshot = true;
    bool Validate = true;
    int Loop = 1;
    std::optional<i64> ShotCount;


    std::optional<TDuration> TimeLimit;
    std::optional<i64> TransferLimit;

    std::vector<TFile> Files;
};

DECLARE_REFCOUNTED_STRUCT(TConfiguration)
DEFINE_REFCOUNTED_TYPE(TConfiguration)

struct IConfigurationGenerator
    : public TRefCounted
{
    virtual void Next() = 0;
    virtual bool IsLast() = 0;
    virtual void Reset() = 0;
};

DECLARE_REFCOUNTED_STRUCT(IConfigurationGenerator)
DEFINE_REFCOUNTED_TYPE(IConfigurationGenerator)

IConfigurationGeneratorPtr CreateDriverConfigurationGenerator(TConfigurationPtr configuration, std::vector<TDriverDescription> drivers);
IConfigurationGeneratorPtr CreateThreadsConfigurationGenerator(TConfigurationPtr configuration, std::vector<int> threads);
IConfigurationGeneratorPtr CreateBlockSizeLogConfigurationGenerator(TConfigurationPtr configuration, std::vector<int> blockSizeLogs);
IConfigurationGeneratorPtr CreateZoneConfigurationGenerator(TConfigurationPtr configuration, std::vector<std::pair<int, int>> ranges);
IConfigurationGeneratorPtr CreatePatternConfigurationGenerator(TConfigurationPtr configuration, std::vector<EPattern> patterns);
IConfigurationGeneratorPtr CreateThrottlerConfigurationGenerator(TConfigurationPtr configuration, std::vector<TCombinedThrottlerConfig> throttlers);
IConfigurationGeneratorPtr CreateReadPercentageConfigurationGenerator(TConfigurationPtr configuration, std::vector<int> readPercentages);
IConfigurationGeneratorPtr CreateDirectConfigurationGenerator(TConfigurationPtr configuration, std::vector<bool> directs);
IConfigurationGeneratorPtr CreateSyncConfigurationGenerator(TConfigurationPtr configuration, std::vector<ESyncMode> syncs);
IConfigurationGeneratorPtr CreateFallocateConfigurationGenerator(TConfigurationPtr configuration, std::vector<EFallocateMode> fallocates);
IConfigurationGeneratorPtr CreateOneshotConfigurationGenerator(TConfigurationPtr configuration, std::vector<bool> oneshots);
IConfigurationGeneratorPtr CreateLoopConfigurationGenerator(TConfigurationPtr configuration, std::vector<int> loops);
IConfigurationGeneratorPtr CreateShotCountConfigurationGenerator(TConfigurationPtr configuration, std::vector<std::optional<i64>> shots);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
