#pragma once
#include <contrib/ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <contrib/ydb/core/tx/schemeshard/olap/operations/alter/abstract/context.h>
#include <contrib/ydb/core/tx/schemeshard/olap/table/table.h>
#include <contrib/ydb/core/formats/arrow/accessor/common/const.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TColumnTableUpdate: public ISSEntityUpdate {
private:
    using TBase = ISSEntityUpdate;

    virtual std::shared_ptr<TColumnTableInfo> GetTargetTableInfo() const = 0;
    virtual std::shared_ptr<ISSEntity> GetTargetSSEntity() const = 0;

    virtual TConclusionStatus DoStart(const TUpdateStartContext& context) override final;
    virtual TConclusionStatus DoFinish(const TUpdateFinishContext& context) override final;

    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SCHEMA;
    }
    virtual TConclusionStatus DoInitializeImpl(const TUpdateInitializationContext& context) = 0;

    bool IsAlterCompression(const TUpdateInitializationContext& context) const {
        for (const auto& alterColumn : context.GetModification()->GetAlterColumnTable().GetAlterSchema().GetAlterColumns()) {
            if (alterColumn.HasSerializer()) {
                return true;
            }
        }
        return false;
    }

protected:
    virtual TConclusionStatus DoStartImpl(const TUpdateStartContext& /*context*/) {
        return TConclusionStatus::Success();
    }
    virtual TConclusionStatus DoFinishImpl(const TUpdateFinishContext& /*context*/) {
        return TConclusionStatus::Success();
    }
    virtual TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override final {
        if (!AppData()->FeatureFlags.GetEnableOlapCompression() && IsAlterCompression(context)) {
            return TConclusionStatus::Fail("Compression is disabled for OLAP tables");
        }
        if (!context.GetModification()->HasAlterColumnTable() && !context.GetModification()->HasAlterTable()) {
            return TConclusionStatus::Fail("no update data");
        }
        return DoInitializeImpl(context);
    }

    std::shared_ptr<TColumnTableInfo> GetTargetTableInfoVerified() const {
        auto result = GetTargetTableInfo();
        AFL_VERIFY(!!result);
        return result;
    }

    template <class T>
    const T& GetTargetEntityAsVerified() const {
        auto resultPtr = dynamic_pointer_cast<T>(GetTargetEntityVerified());
        AFL_VERIFY(!!resultPtr);
        return *resultPtr;
    }

    std::shared_ptr<ISSEntity> GetTargetEntityVerified() const {
        auto result = GetTargetSSEntity();
        AFL_VERIFY(!!result);
        return result;
    }

    bool CheckTargetSchema(const TOlapSchema& targetSchema) {
        if (!AppData()->FeatureFlags.GetEnableSparsedColumns()) {
            for (auto& [_, column] : targetSchema.GetColumns().GetColumns()) {
                if (column.GetDefaultValue().GetValue() || (column.GetAccessorConstructor().GetClassName() == NKikimr::NArrow::NAccessor::TGlobalConst::SparsedDataAccessorName)) {
                    return false;
                }
            }
        }
        return true;
    }

public:
};

}