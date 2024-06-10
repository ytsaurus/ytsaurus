#pragma once
#include <bigrt/lib/supplier/common/message_chunk_pusher.h>
#include <bigrt/lib/supplier/interface/supplier.h>
#include <bigrt/lib/supplier/supplier.h>
#include <yt/cpp/roren/library/timers/timers.h>

namespace NRoren::NPrivate
{
extern const TString SYSTEM_TIMERS_SUPPLIER_NAME;

struct TSystemTimersSupplierConfig
{
    std::function<NPrivate::TTimers& (const uint64_t)> GetTimers;
    int64_t ShardsCount = -1;
};

class TSystemTimersSupplier
    : public NBigRT::TSupplier
{
public:
    TSystemTimersSupplier(const TSystemTimersSupplierConfig& config, const ui64 shard);
    void CommitOffset(TStringBuf) noexcept override;
    void SupplyUntilStopped(NBSYeti::TStopToken stopToken, TStringBuf startOffset, TCallback pushCallback) noexcept override;
protected:
    const TSystemTimersSupplierConfig Config_;
    std::reference_wrapper<NPrivate::TTimers> Timers_;
    int64_t Offset_ = -1;
};

class TSystemTimersSupplierFactory
    : public NBigRT::TSupplierFactory
{
public:
    TSystemTimersSupplierFactory(TSystemTimersSupplierConfig config);
    ui64 GetShardsCount() override;
    NBigRT::TSupplierPtr CreateSupplier(ui64 shard) override;
protected:
    const TSystemTimersSupplierConfig Config_;
};

NBigRT::TSupplierFactoryPtr CreateSystemTimersSupplierFactory(TSystemTimersSupplierConfig config);
NBigRT::TSupplierFactoriesProvider CreateExSupplierFactoriesProvider(
    NBigRT::TSupplierFactoriesData data,
    std::function<NPrivate::TTimers& (const uint64_t, const NBigRT::TSupplierFactoryContext&)> getTimers);

}  // namespace NRoren::NPrivate
