#include "public.h"

#include "data_slice_descriptor.h"

#include <yt/yt/client/ypath/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Merge adjacent data slices and encode them into a #TRichYPath.
//! Fails if #dataSourceDirectory and #slicesByTable have different sizes.
//! If you want the resulting table paths to have the same attributes as
//! input table paths specify the #paths argument containing
//! input table paths and their attributes.
std::vector<NYPath::TRichYPath> CombineDataSlices(
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    std::vector<std::vector<TDataSliceDescriptor>>& slicesByTable,
    const std::optional<std::vector<NYPath::TRichYPath>>& paths = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
