#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <optional>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TOneShotFlag
{
public:
    void Set(bool value) noexcept;
    bool Get() const noexcept;

    void operator=(bool value) noexcept;

    operator bool () const noexcept;

private:
    std::optional<bool> Flag_;
};

// NB(pogorelov): Doesn't need to be an atomic,
// cause it can be modified only once (and its modification will happen before it can be read).
inline TOneShotFlag DeliveryFencedWriteEnabled;

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemalessMultiChunkWriterPtr CreateJobShuffleWriter(
    const IJobHostPtr& host,
    const NControllerAgent::NProto::TPartitionJobSpecExt& partitionJobSpecExt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
