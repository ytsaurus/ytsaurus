#pragma once

#include <yt/yt/flow/library/cpp/common/describe_traits.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseDescribeTraits
    : public TDescribeTraitsBase
{
public:
    using TDescribeTraitsBase::TDescribeTraitsBase;

    void MakeLinks(const NYTree::IMapNodePtr& parameters) const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
