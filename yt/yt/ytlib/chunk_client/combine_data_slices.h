#include "public.h"

#include "data_slice_descriptor.h"

#include <yt/yt/client/ypath/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Merge adjacent data slices and encode them into a TRichYPath.
//! Fails if dataSourceDirectory and slicesByTable have different sizes.
std::vector<NYPath::TRichYPath> CombineDataSlices(
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    std::vector<std::vector<TDataSliceDescriptor>>& slicesByTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
