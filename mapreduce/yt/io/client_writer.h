#pragma once

#include "proxy_output.h"

#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/interface/io.h>

namespace NYT {

struct TTableWriterOptions;
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
        EDataStreamFormat format,
        const Stroka& formatConfig,
        const TTableWriterOptions& options);

    size_t GetStreamCount() const override;
    TOutputStream* GetStream(size_t tableIndex) const override;
    void OnRowFinished(size_t tableIndex) override;

private:
    ::TIntrusivePtr<TBlockWriter> BlockWriter_;

    static const size_t BUFFER_SIZE = 64 << 20;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
