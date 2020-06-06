#include "arrow_row_stream.h"

#include <yt/client/api/rpc_proxy/row_stream.h>
#include <yt/client/api/rpc_proxy/wire_row_stream.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/range.h>

namespace NYT::NArrow {

using namespace NApi::NRpcProxy;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TArrowRowStreamFormatter
    : public IRowStreamFormatter
{
public:
    explicit TArrowRowStreamFormatter(TNameTablePtr nameTable)
        : FallbackFormatter_(CreateWireRowStreamFormatter(std::move(nameTable)))
    { }
    
    virtual TSharedRef Format(
        const IUnversionedRowBatchPtr& batch,
        const NApi::NRpcProxy::NProto::TRowsetStatistics* statistics) override
    {
        return FallbackFormatter_->Format(batch, statistics);
    }

private:
    const IRowStreamFormatterPtr FallbackFormatter_;
};

IRowStreamFormatterPtr CreateArrowRowStreamFormatter(TNameTablePtr nameTable)
{
    return New<TArrowRowStreamFormatter>(std::move(nameTable));
}

////////////////////////////////////////////////////////////////////////////////

IRowStreamParserPtr CreateArrowRowStreamParser(TNameTablePtr /*nameTable*/)
{
    THROW_ERROR_EXCEPTION("Arrow parser is not implemented yet");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow

