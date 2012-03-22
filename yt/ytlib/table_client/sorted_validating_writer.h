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
        IAsyncWriter* writer);

    TAsyncError AsyncEndRow();

private:
    TKey PreviousKey;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
