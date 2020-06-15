#pragma once

#include "private.h"

#include <Processors/ISimpleTransform.h>

namespace NYT::NClickHouseServer
{

////////////////////////////////////////////////////////////////////////////////

class TLoggingTransform
    : public DB::ISimpleTransform
{
public:
    TLoggingTransform(const DB::Block& header, const NLogging::TLogger& logger);

    virtual std::string getName() const override;

protected:
    virtual void transform(DB::Chunk & chunk) override;

private:
    NLogging::TLogger Logger;
    TInstant LastLogTime = TInstant::Zero();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
