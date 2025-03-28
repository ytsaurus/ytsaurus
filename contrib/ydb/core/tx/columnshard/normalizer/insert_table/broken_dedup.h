#pragma once

#include <contrib/ydb/core/tx/columnshard/columnshard_schema.h>
#include <contrib/ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap::NInsertionDedup {

class TInsertionsDedupNormalizer: public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;

public:
    static TString GetClassNameStatic() {
        return "CleanInsertionDedup";
    }

private:
    class TNormalizerResult;

    static const inline INormalizerComponent::TFactory::TRegistrator<TInsertionsDedupNormalizer> Registrator =
        INormalizerComponent::TFactory::TRegistrator<TInsertionsDedupNormalizer>(GetClassNameStatic());

public:
    TInsertionsDedupNormalizer(const TNormalizationController::TInitContext& context)
        : TBase(context) {
    }

    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return ENormalizerSequentialId::CleanInsertionDedup;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}   // namespace NKikimr::NOlap::NInsertionDedup
