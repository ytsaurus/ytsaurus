#pragma once

#include <contrib/ydb/services/metadata/abstract/initialization.h>


namespace NKikimr::NKqp {

class TResourcePoolClassifierInitializer : public NMetadata::NInitializer::IInitializationBehaviour {
protected:
    virtual void DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const override;
};

}  // namespace NKikimr::NKqp
