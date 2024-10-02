#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NObjects::NDetail {

////////////////////////////////////////////////////////////////////////////////

struct IEntropySource
{
    virtual ~IEntropySource() = default;
    virtual EAttributeGenerationPolicy GetGenerationPolicy() const = 0;
    virtual ui64 Get() = 0;
    virtual ui64 GetMinValue() const = 0;
    virtual ui64 GetMaxValue() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IEntropySource> MakeRandomEntropySource();
std::unique_ptr<IEntropySource> MakeTimestampEntropySource(NMaster::IBootstrap* bootstrap);
std::unique_ptr<IEntropySource> MakeBufferedTimestampEntropySource(NMaster::IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects::NDetail
