#pragma once

#include "snapshot.h"
#include <contrib/ydb/services/metadata/abstract/common.h>
#include <contrib/ydb/services/metadata/abstract/initialization.h>
#include <contrib/ydb/services/metadata/manager/abstract.h>
#include <contrib/ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NInitializer {

class TInitializer: public IInitializationBehaviour {
protected:
    virtual void DoPrepare(IInitializerInput::TPtr controller) const override;
public:
};

}
