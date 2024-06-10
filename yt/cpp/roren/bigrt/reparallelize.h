#pragma once

#include <yt/cpp/roren/interface/transforms.h>

#include <util/generic/ptr.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TReparallelizeTransform;

enum EConcurrencyMode
{
    Serialized,
    Concurrent,
    // Subsharded, // TODO
};

class IParallelizationConfig
    : public TThrRefBase
{
public:
    int InputBatchSize = -1;

public:
    virtual EConcurrencyMode GetType() const = 0;
};

using IParallelizationConfigPtr = ::TIntrusivePtr<IParallelizationConfig>;

///
/// @brief Change parallelization mode
///
/// This transform doesn't change data but changes parallelization mode for all
/// transforms subsequent transforms.
TReparallelizeTransform Reparallelize(IParallelizationConfigPtr config);

/// @brief Create config for serialized execution.
IParallelizationConfigPtr SerializedExecution();

/// @brief Create config for concurrent execution.
IParallelizationConfigPtr ConcurrentExecution();

class TReparallelizeTransform
{
public:
    explicit TReparallelizeTransform(IParallelizationConfigPtr config)
        : Config_(std::move(config))
    { }

    template <typename TRow>
    TPCollection<TRow> ApplyTo(const TPCollection<TRow>& pCollection) const;

private:
    IParallelizationConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

extern const TTypeTag<IParallelizationConfigPtr> ParallelizationConfigTag;

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

//
// IMPLEMENTATION
//

template <typename TRow>
TPCollection<TRow> TReparallelizeTransform::ApplyTo(const TPCollection<TRow> &pCollection) const
{
    auto result = Flatten({pCollection});
    NPrivate::SetAttribute(*NPrivate::GetRawDataNode(result), NPrivate::ParallelizationConfigTag, Config_);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
