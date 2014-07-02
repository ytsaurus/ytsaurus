#pragma once

#include "public.h"

#include <core/yson/consumer.h>
#include <core/yson/parser.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TFormatsConsumerBase
    : public virtual NYson::IYsonConsumer
{
public:
    TFormatsConsumerBase();

    // This method has standard implementation for yamr, dsv and yamred dsv formats.
    virtual void OnRaw(const TStringBuf& yson, NYson::EYsonType type) override;

private:
    NYson::TStatelessYsonParser Parser;
};

////////////////////////////////////////////////////////////////////////////////

bool IsSpecialJsonKey(const TStringBuf& str);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
