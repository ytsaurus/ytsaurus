#pragma once

#include <mapreduce/yt/common/fwd.h>

#include <mapreduce/yt/io/proxy_output.h>

#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/interface/io.h>

namespace NYT {

struct TTableWriterOptions;
class TRetryfulWriter;

////////////////////////////////////////////////////////////////////////////////

class TClientWriter
    : public TProxyOutput
{
public:
    TClientWriter(
        const TRichYPath& path,
        IClientRetryPolicyPtr clientRetryPolicy,
        ITransactionPingerPtr transactionPinger,
        const TClientContext& context,
        const TTransactionId& transactionId,
        const TMaybe<TFormat>& format,
        const TTableWriterOptions& options);

    size_t GetStreamCount() const override;
    IOutputStream* GetStream(size_t tableIndex) const override;
    void OnRowFinished(size_t tableIndex) override;
    void Abort() override;

private:
    ::TIntrusivePtr<TRawTableWriter> RawWriter_;

    const size_t BUFFER_SIZE = 64 << 20;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
