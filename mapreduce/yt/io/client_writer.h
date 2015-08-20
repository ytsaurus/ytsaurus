#pragma once

#include "proxy_output.h"

#include <mapreduce/yt/http/requests.h>

namespace NYT {

class TBlockWriter;

////////////////////////////////////////////////////////////////////////////////

class TClientWriter
    : public TProxyOutput
{
public:
    TClientWriter(
        const TRichYPath& path,
        const TAuth& auth,
        const TTransactionId& transactionId,
        EDataStreamFormat format);

    virtual size_t GetStreamCount() const override;
    virtual TOutputStream* GetStream(size_t tableIndex) override;
    virtual void OnRowFinished(size_t tableIndex) override;

private:
    THolder<TBlockWriter> BlockWriter_;

    static const size_t BUFFER_SIZE = 64 << 20;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
