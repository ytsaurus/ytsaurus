#include "table_importer.h"

#include <yt/yt/client/api/table_importer.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTableImporter
    : public ITableImporter
{
public:
    TTableImporter(
        IAsyncZeroCopyOutputStreamPtr underlying,
        TTableSchemaPtr schema)
        : Underlying_(std::move(underlying))
        , Schema_(std::move(schema))
    {
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    TFuture<void> Close() override
    {
        return Underlying_->Close();
    }

private:
    const NConcurrency::IAsyncZeroCopyOutputStreamPtr Underlying_;
    const TTableSchemaPtr Schema_;
};

ITableImporterPtr CreateTableImporter(
    IAsyncZeroCopyOutputStreamPtr outputStream,
    TTableSchemaPtr schema)
{
    return New<TTableImporter>(std::move(outputStream), std::move(schema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
