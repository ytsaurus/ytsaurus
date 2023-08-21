#pragma once

#include "config.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TBlockReorderer
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<int>, BlockIndexMapping);

public:
    explicit TBlockReorderer(TBlockReordererConfigPtr config);

    //! Reorder given blocks and save index mapping to BlockIndexMapping.
    //! May be called multiple times, but same block index should not appear
    //! twice between calls.
    void ReorderBlocks(std::vector<TBlock>& blocks);

    bool IsEnabled() const;

private:
    const TBlockReordererConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
