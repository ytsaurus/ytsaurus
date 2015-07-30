#pragma once

#include "proxy_output.h"

#include <mapreduce/yt/http/http.h>

namespace NYT {

class TBlockWriter;

////////////////////////////////////////////////////////////////////////////////

class TClientWriter
    : public TProxyOutput
{
public:
    TClientWriter(
        const TRichYPath& path,
        const Stroka& serverName,
        const TTransactionId& transactionId,
        EDataStreamFormat format);

    virtual size_t GetStreamCount() const override;
    virtual TOutputStream* GetStream(size_t tableIndex) override;
    virtual void OnRowFinished(size_t tableIndex) override;

private:
    TRichYPath Path_;
    Stroka ServerName_;
    TTransactionId TransactionId_;
    EDataStreamFormat Format_;

    THolder<TBlockWriter> BlockWriter_;

    static const size_t BUFFER_SIZE = 64 << 20;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
