#pragma once
#include <contrib/ydb/services/metadata/abstract/common.h>
#include <contrib/ydb/services/metadata/initializer/common.h>
#include <contrib/ydb/services/metadata/abstract/initialization.h>

namespace NKikimr::NColumnShard::NTiers {

class TTiersInitializer: public NMetadata::NInitializer::IInitializationBehaviour {
protected:
    TVector<NMetadata::NInitializer::ITableModifier::TPtr> BuildModifiers() const;
    virtual void DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const override;
public:
};

}
