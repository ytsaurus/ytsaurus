#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NQueryClient {

struct ICGInstanceHolder : public virtual TRefCounted {
    virtual void Run(
        NTableClient::TUnversionedValue* value,
        NTableClient::TUnversionedRow schemafulRow,
        const NTableClient::TRowBufferPtr& rowBuffer) = 0;
};

using ICGInstanceHolderPtr = TIntrusivePtr<ICGInstanceHolder>;

std::pair<ICGInstanceHolderPtr, NTableClient::TTableSchemaPtr> MakeRlsCGInstance(const NTableClient::TTableSchemaPtr& schema, const std::string& predicate);

}
