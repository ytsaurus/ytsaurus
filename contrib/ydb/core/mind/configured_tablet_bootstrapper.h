#pragma once
#include "defs.h"
#include <contrib/ydb/core/tablet/tablet_setup.h>
#include <contrib/ydb/core/base/tablet_types.h>
#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/protos/bootstrap.pb.h>

namespace NKikimr {

    // would subscribe to boot config and instantiate tablet bootstrapper if configured for this node
    IActor* CreateConfiguredTabletBootstrapper(const ::NKikimrConfig::TBootstrap::TTablet &defaultConfig);

    TTabletTypes::EType BootstrapperTypeToTabletType(ui32 type);
    TIntrusivePtr<TTabletSetupInfo> MakeTabletSetupInfo(TTabletTypes::EType tabletType, ui32 poolId, ui32 tabletPoolId);
}
