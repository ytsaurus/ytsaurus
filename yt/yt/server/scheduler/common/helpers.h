#pragma once

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/yson/forwarding_consumer.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TYsonMapFragmentBatcher final
    : public NYson::TForwardingYsonConsumer
    , public NYson::IFlushableYsonConsumer
    , private TNonCopyable
{
public:
    TYsonMapFragmentBatcher(
        std::vector<NYson::TYsonString>* batchOutput,
        int maxBatchSize,
        NYson::EYsonFormat format = NYson::EYsonFormat::Binary);

    //! Flushes current batch if it's non-empty.
    void Flush() override;

protected:
    void OnMyKeyedItem(TStringBuf key) override;

private:
    std::vector<NYson::TYsonString>* const BatchOutput_;
    const int MaxBatchSize_;

    int BatchSize_ = 0;
    TStringStream BatchStream_;
    std::unique_ptr<NYson::IFlushableYsonConsumer> BatchWriter_;
};

////////////////////////////////////////////////////////////////////////////////

static inline constexpr int InvalidTreeSetTopologyVersion = -1;
static inline constexpr int InvalidTreeIndex = -1;

// TODO(eshcherbin): Refactor and move to strategy.
struct TMatchingTreeCookie
{

    int TreeSetTopologyVersion = InvalidTreeSetTopologyVersion;
    int TreeIndex = InvalidTreeIndex;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
