#include "common.h"
#include "validating_writer.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSortedValidatingWriter
    : public TValidatingWriter
{
public:
    TSortedValidatingWriter(
        const TSchema& schema,
        std::vector<TColumn>&& keyColumns,
        IAsyncWriter* writer);

    TAsyncError::TPtr AsyncEndRow();

private:
    TKey PreviousKey;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
