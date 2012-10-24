#pragma once

#include "public.h"
#include "yson_consumer.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Describes an attribute filtering mode.
DECLARE_ENUM(EAttributeFilterMode,
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
    TAttributeFilter()
        : Mode(EAttributeFilterMode::None)
    { }

    TAttributeFilter(EAttributeFilterMode mode, const std::vector<Stroka>& keys)
        : Mode(mode)
        , Keys(keys)
    { }

    EAttributeFilterMode Mode;
    std::vector<Stroka> Keys;

    static TAttributeFilter All;
    static TAttributeFilter None;
};

////////////////////////////////////////////////////////////////////////////////

struct IAttributeProvider
{
    virtual ~IAttributeProvider()
    { }

    //! Writes attributes that match #filter into #consumer.
    virtual void GetAttributes(
        IYsonConsumer* consumer,
        const TAttributeFilter& filter) const = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
