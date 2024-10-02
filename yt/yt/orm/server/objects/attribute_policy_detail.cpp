#include "attribute_policy_detail.h"

#include "transaction_manager.h"

#include <yt/yt/orm/server/master/bootstrap.h>

namespace NYT::NOrm::NServer::NObjects::NDetail {

using namespace NConcurrency;
using NMaster::IBootstrap;

////////////////////////////////////////////////////////////////////////////////

class TRandomEntropySource
    : public IEntropySource
{
public:
    EAttributeGenerationPolicy GetGenerationPolicy() const override
    {
        return EAttributeGenerationPolicy::Random;
    }

    ui64 Get() override
    {
        return Offset + RandomNumber<ui64>(Range);
    }

    ui64 GetMinValue() const override
    {
        return Offset;
    }

    ui64 GetMaxValue() const override
    {
        return Offset + Range - 1;
    }

private:
    // Random ints shall be:
    // - 64-bit
    // - positive when signed
    // - away from zero and 32-bit ints
    // - away from max int and current YT timestamps (which are greater than 2^60)
    inline static const ui64 Offset = 1ULL << 33ULL;
    inline static const ui64 Range = 1ULL << 59ULL;
};

////////////////////////////////////////////////////////////////////////////////

class TTimestampEntropySourceBase
    : public IEntropySource
{
public:
    ui64 GetMinValue() const override
    {
        // This is greater than MinTimestamp, but we won't be pretending it's the 1970s, so we'll
        // let clients set appropriate validation ranges.
        return 1ULL << 60ULL;
    }

    ui64 GetMaxValue() const override
    {
        return NTransactionClient::MaxTimestamp;
    }
};

class TTimestampEntropySource
    : public TTimestampEntropySourceBase
{
public:
    TTimestampEntropySource(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    EAttributeGenerationPolicy GetGenerationPolicy() const override
    {
        return EAttributeGenerationPolicy::Timestamp;
    }

    ui64 Get() override
    {
        return WaitFor(Bootstrap_->GetTransactionManager()->GenerateTimestamps())
            .ValueOrThrow();
    }

private:
    IBootstrap* const Bootstrap_;
};

class TBufferedTimestampEntropySource
    : public TTimestampEntropySourceBase
{
public:
    TBufferedTimestampEntropySource(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    EAttributeGenerationPolicy GetGenerationPolicy() const override
    {
        return EAttributeGenerationPolicy::BufferedTimestamp;
    }

    ui64 Get() override
    {
        return Bootstrap_->GetTransactionManager()->GenerateTimestampBuffered();
    }

private:
    IBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IEntropySource> MakeRandomEntropySource()
{
    return std::make_unique<TRandomEntropySource>();
}

std::unique_ptr<IEntropySource> MakeTimestampEntropySource(IBootstrap* bootstrap)
{
    return std::make_unique<TTimestampEntropySource>(bootstrap);
}

std::unique_ptr<IEntropySource> MakeBufferedTimestampEntropySource(IBootstrap* bootstrap)
{
    return std::make_unique<TBufferedTimestampEntropySource>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects::NDetail
