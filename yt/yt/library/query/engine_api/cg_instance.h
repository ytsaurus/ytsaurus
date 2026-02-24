#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableClient {

struct ICGInstanceHolder {
    virtual void Run(
        TUnversionedValue* value,
        TUnversionedRow schemafulRow,
        const TRowBufferPtr& rowBuffer) = 0;
};

using ICGInstanceHolderPtr = TIntrusivePtr<ICGInstanceHolder>;

}
