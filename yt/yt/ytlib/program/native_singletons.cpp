#include "native_singletons.h"

#include "config.h"

#include <yt/yt/ytlib/auth/native_authentication_manager.h>

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/library/containers/porto_resource_tracker.h>

#include <yt/yt/library/disk_manager/hotswap_manager.h>

#include <yt/yt/library/program/helpers.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNativeSingletonsManager
{
public:
    static TNativeSingletonsManager* Get()
    {
        return LeakySingleton<TNativeSingletonsManager>();
    }

    void ConfigureNativeSingletons(const TNativeSingletonsConfigPtr& config)
    {
        {
            auto guard = Guard(Lock_);

            if (std::exchange(Configured_, true)) {
                THROW_ERROR_EXCEPTION("Native singletons have already been configured");
            }

            Config_ = config;
        }

        ConfigureSingletons(static_cast<TSingletonsConfigPtr>(config));

        NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher);
        NAuth::TNativeAuthenticationManager::Get()->Configure(config->NativeAuthenticationManager);
    }

    void ReconfigureNativeSingletons(const TNativeSingletonsDynamicConfigPtr& dynamicConfig)
    {
        auto guard = Guard(Lock_);

        if (!Configured_) {
            THROW_ERROR_EXCEPTION("Native singletons are not configured yet");
        }

        ReconfigureSingletons(
            static_cast<TSingletonsConfigPtr>(Config_),
            static_cast<TSingletonsDynamicConfigPtr>(dynamicConfig));

        NChunkClient::TDispatcher::Get()->Configure(Config_->ChunkClientDispatcher->ApplyDynamic(dynamicConfig->ChunkClientDispatcher));
        NAuth::TNativeAuthenticationManager::Get()->Reconfigure(dynamicConfig->NativeAuthenticationManager);

        if (dynamicConfig->HotswapManager) {
            NDiskManager::THotswapManager::Reconfigure(dynamicConfig->HotswapManager);
        }
    }

private:
    DECLARE_LEAKY_SINGLETON_FRIEND();

    TNativeSingletonsManager() = default;

    NThreading::TSpinLock Lock_;
    bool Configured_ = false;
    TNativeSingletonsConfigPtr Config_;
};

void ConfigureNativeSingletons(const TNativeSingletonsConfigPtr& config)
{
    TNativeSingletonsManager::Get()->ConfigureNativeSingletons(config);
}

void ReconfigureNativeSingletons(const TNativeSingletonsDynamicConfigPtr& dynamicConfig)
{
    TNativeSingletonsManager::Get()->ReconfigureNativeSingletons(dynamicConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
