#pragma once

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/core/yson/public.h>

#include <util/generic/ptr.h>


namespace NYql::NYtflow::NCodec::NPrivate {

struct IValueSkipper {
public:
    virtual void SkipValue(
        NYT::NYson::TYsonPullParser& ysonParser,
        const NYT::NTableClient::TLogicalType* ytType) const = 0;

public:
    virtual ~IValueSkipper() = default;
};

THolder<IValueSkipper> CreateValueSkipper();

} // namespace NYql::NYtflow::NCodec::NPrivate
