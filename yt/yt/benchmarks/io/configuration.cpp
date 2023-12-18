#include "configuration.h"
#include <yt/yt/core/misc/common.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

template <typename T, T TConfiguration::* M>
class TConfigurationGenerator
    : public IConfigurationGenerator
{
public:
    TConfigurationGenerator(TConfigurationPtr configuration, std::vector<T> items)
        : Configuration_(std::move(configuration))
        , Items_(std::move(items))
    {
        Reset();
    }

    void Next() override
    {
        if (!IsLast()) {
            Configuration_.Get()->*M = Items_[Position_++];
        }
    }

    bool IsLast() override
    {
        return Position_ == Items_.size();
    }

    void Reset() override
    {
        Position_ = 0;
        Next();
    }

private:
    const TConfigurationPtr Configuration_;
    std::vector<T> Items_;
    size_t Position_;
};

////////////////////////////////////////////////////////////////////////////////

IConfigurationGeneratorPtr CreateDriverConfigurationGenerator(TConfigurationPtr configuration, std::vector<TDriverDescription> drivers)
{
    return New<TConfigurationGenerator<TDriverDescription, &TConfiguration::Driver>>(std::move(configuration), std::move(drivers));
}

IConfigurationGeneratorPtr CreateThreadsConfigurationGenerator(TConfigurationPtr configuration, std::vector<int> threads)
{
    return New<TConfigurationGenerator<int, &TConfiguration::Threads>>(std::move(configuration), std::move(threads));
}

IConfigurationGeneratorPtr CreateBlockSizeLogConfigurationGenerator(TConfigurationPtr configuration, std::vector<int> blockSizeLogs)
{
    return New<TConfigurationGenerator<int, &TConfiguration::BlockSizeLog>>(std::move(configuration), std::move(blockSizeLogs));
}

IConfigurationGeneratorPtr CreateZoneConfigurationGenerator(TConfigurationPtr configuration, std::vector<std::pair<int, int>> ranges)
{
    return New<TConfigurationGenerator<std::pair<int, int>, &TConfiguration::Zone>>(std::move(configuration), std::move(ranges));
}

IConfigurationGeneratorPtr CreatePatternConfigurationGenerator(TConfigurationPtr configuration, std::vector<EPattern> patterns)
{
    return New<TConfigurationGenerator<EPattern, &TConfiguration::Pattern>>(std::move(configuration), std::move(patterns));
}

IConfigurationGeneratorPtr CreateThrottlerConfigurationGenerator(TConfigurationPtr configuration, std::vector<TCombinedThrottlerConfig> throttlers)
{
    return New<TConfigurationGenerator<TCombinedThrottlerConfig, &TConfiguration::Throttler>>(std::move(configuration), std::move(throttlers));
}

IConfigurationGeneratorPtr CreateReadPercentageConfigurationGenerator(TConfigurationPtr configuration, std::vector<int> readPercentages)
{
    return New<TConfigurationGenerator<int, &TConfiguration::ReadPercentage>>(std::move(configuration), std::move(readPercentages));
}

IConfigurationGeneratorPtr CreateDirectConfigurationGenerator(TConfigurationPtr configuration, std::vector<bool> directs)
{
    return New<TConfigurationGenerator<bool, &TConfiguration::Direct>>(std::move(configuration), std::move(directs));
}

IConfigurationGeneratorPtr CreateSyncConfigurationGenerator(TConfigurationPtr configuration, std::vector<ESyncMode> syncs)
{
    return New<TConfigurationGenerator<ESyncMode, &TConfiguration::Sync>>(std::move(configuration), std::move(syncs));
}

IConfigurationGeneratorPtr CreateFallocateConfigurationGenerator(TConfigurationPtr configuration, std::vector<EFallocateMode> fallocates)
{
    return New<TConfigurationGenerator<EFallocateMode, &TConfiguration::Fallocate>>(std::move(configuration), std::move(fallocates));
}

IConfigurationGeneratorPtr CreateOneshotConfigurationGenerator(TConfigurationPtr configuration, std::vector<bool> oneshots)
{
    return New<TConfigurationGenerator<bool, &TConfiguration::Oneshot>>(std::move(configuration), std::move(oneshots));
}

IConfigurationGeneratorPtr CreateLoopConfigurationGenerator(TConfigurationPtr configuration, std::vector<int> loops)
{
    return New<TConfigurationGenerator<int, &TConfiguration::Loop>>(std::move(configuration), std::move(loops));
}

IConfigurationGeneratorPtr CreateShotCountConfigurationGenerator(TConfigurationPtr configuration, std::vector<std::optional<i64>> shots)
{
    return New<TConfigurationGenerator<std::optional<i64>, &TConfiguration::ShotCount>>(std::move(configuration), std::move(shots));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
