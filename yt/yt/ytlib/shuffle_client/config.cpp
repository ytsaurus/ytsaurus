#include "config.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>

#include <yt/yt/ytlib/push_based_shuffle_client/config.h>

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/shuffle_client.h>

#include <yt/yt/client/table_client/config.h>

namespace NYT::NShuffleClient {

////////////////////////////////////////////////////////////////////////////////

void TPullShuffleConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reader", &TThis::Reader)
        .DefaultNew();
    registrar.Parameter("writer", &TThis::Writer)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TPushShuffleConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("writer", &TThis::Writer)
        .DefaultNew();
    registrar.Parameter("reader", &TThis::Reader)
        .DefaultNew();
    registrar.Parameter("journal_writer", &TThis::JournalWriter)
        .DefaultNew();
    registrar.Parameter("session_pool", &TThis::SessionPool)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TShuffleConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("pull", &TThis::Pull)
        .Default();
    registrar.Parameter("push", &TThis::Push)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void EnsureModeSection(const TShuffleConfigPtr& config, bool usePushBasedShuffle)
{
    if (usePushBasedShuffle) {
        if (!config->Push) {
            config->Push = New<TPushShuffleConfig>();
        }
    } else {
        if (!config->Pull) {
            config->Pull = New<TPullShuffleConfig>();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TShuffleConfigPtr GetShuffleConfig(const NApi::TShuffleHandlePtr& handle)
{
    auto config = handle->Config
        ? NYTree::ConvertTo<TShuffleConfigPtr>(*handle->Config)
        : New<TShuffleConfig>();

    EnsureModeSection(config, handle->UsePushBasedShuffle);

    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleClient
