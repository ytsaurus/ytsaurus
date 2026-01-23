#pragma once

#include <contrib/ydb/services/metadata/abstract/common.h>
#include <contrib/ydb/services/metadata/abstract/initialization.h>
#include <contrib/ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NCSIndex {

class TInitializer: public NInitializer::IInitializationBehaviour {
protected:
    virtual void DoPrepare(NInitializer::IInitializerInput::TPtr controller) const override;
public:
};

}
