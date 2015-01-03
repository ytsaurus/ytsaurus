#pragma once

#include "public.h"

#include <core/yson/consumer.h>

#include <core/ytree/ypath.pb.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Describes an attribute filtering mode.
DEFINE_ENUM(EAttributeFilterMode,
    // Accept all attributes.
    (All)
    // Don't accept any attribute.
    (None)
    // Accept only matching attributes.
    (MatchingOnly)
);

//! Describes a filtering criteria for attributes.
/*!
 *  If #Mode is |All| or |None| then act accordingly.
 *  If #Mode is |MatchingOnly| then only accept keys listed in #Keys.
 */
struct TAttributeFilter
{
    TAttributeFilter();
    TAttributeFilter(EAttributeFilterMode mode, const std::vector<Stroka>& keys);
    explicit TAttributeFilter(EAttributeFilterMode mode);

    EAttributeFilterMode Mode;
    std::vector<Stroka> Keys;

    static TAttributeFilter All;
    static TAttributeFilter None;
};

void ToProto(NProto::TAttributeFilter* protoFilter, const TAttributeFilter& filter);
void FromProto(TAttributeFilter* filter, const NProto::TAttributeFilter& protoFilter);

////////////////////////////////////////////////////////////////////////////////

struct IAttributeProvider
{
    virtual ~IAttributeProvider()
    { }

    //! Writes attributes that match #filter into #consumer.
    virtual void SerializeAttributes(
        NYson::IYsonConsumer* consumer,
        const TAttributeFilter& filter,
        bool sortKeys) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
