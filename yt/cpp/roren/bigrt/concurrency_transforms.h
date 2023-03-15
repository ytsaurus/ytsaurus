#pragma once

#include "fwd.h"

#include <yt/cpp/roren/interface/roren.h>

#include <variant>

///
/// @file concurrency_transforms.h
///
/// Разные части BigRt pipeline'а могут исполнятся в различных режимах параллельности.
/// Основные режимы такие:
///  - Serialized режим. В этом режиме строки обрабатываются "в один поток",
///    т.е. одновременно может обрабатываться только одна строка.
///  - Concurrent режим. В этом режиме одновременно могут обрабатываться несколько строк.
///
/// По-умолчанию используется Serialized режим.
///
/// Специальные служебные transform'ы StartConcurrencyBlock начинают новый блок,
/// который будет исполнятся в соответствии с переданной в transform конфигурацией.
///
/// Пример:
/// ```
///   pipeleine.ReadMessageBatch()
///   | parDoA // parDoA is executed in default serialized mode
///   | StartConcurrencyMode(TConcurrentBlockConfig{}) // starts new concurrent block
///   | parDoB //
///   | parDoC // parDoB and parDoC are executed in Concurrent mode
///   | StartConcurrencyMode(TSerializedBlockConifg{}) // starts new serialized block
///   | ...
/// ```

namespace NRoren {
class TStartConcurrencyBlockTransform;

////////////////////////////////////////////////////////////////////////////////

/// TODO: rename to TBlockInputConfig ?
struct TBlockPipeConfig
{
    /// TODO: rename to DesiredInputRowCount ?
    ssize_t DesiredInputElementCount = 0;
};

struct TSerializedBlockConfig
    : public TBlockPipeConfig
{ };

struct TConcurrentBlockConfig
    : public TBlockPipeConfig
{ };

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Start new "serialized execution" block.
///
/// This transform doesn't change input elements but switches concurrency mode for following transforms.
/// For pipelines other than BigRt this transfrom is effectively noop (and usually optimized out).
TStartConcurrencyBlockTransform StartConcurrencyBlock(const TSerializedBlockConfig& config);

///
/// @brief Start new "concurrent execution" block.
///
/// This transform doesn't change input elements but switches concurrency mode for following transforms.
/// For pipelines other than BigRt this transfrom is effectively noop (and usually optimized out).
TStartConcurrencyBlockTransform StartConcurrencyBlock(const TConcurrentBlockConfig& config);

////////////////////////////////////////////////////////////////////////////////
// IMPLEMENTATION
////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

using TConcurrencyBlockConfig = std::variant<TSerializedBlockConfig, TConcurrentBlockConfig>;
using TExecutionBlockConfig = std::variant<TSerializedBlockConfig, TConcurrentBlockConfig>; // TODO: remove in favour of previous typedef
extern TTypeTag<TExecutionBlockConfig> ExecutionBlockConfigTag;

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

class TStartConcurrencyBlockTransform
{
public:
    TStartConcurrencyBlockTransform(NPrivate::TConcurrencyBlockConfig config);

    template <typename TRow>
    TPCollection<TRow> ApplyTo(const TPCollection<TRow>& pCollection) const
    {
        auto flatten = Flatten();
        NPrivate::SetAttribute(flatten, NPrivate::ExecutionBlockConfigTag, Config_);
        return pCollection | flatten;
    }

private:
    NPrivate::TConcurrencyBlockConfig Config_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
