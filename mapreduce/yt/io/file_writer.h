#pragma once

#include "block_writer.h"

#include <mapreduce/yt/interface/io.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TFileWriter
    : public IFileWriter
{
public:
    TFileWriter(
        const TRichYPath& path,
        const TAuth& auth,
        const TTransactionId& transactionId);

protected:
    virtual void DoWrite(const void* buf, size_t len) override;
    virtual void DoFinish() override;

private:
    TBlockWriter BlockWriter_;

    static const size_t BUFFER_SIZE = 64 << 20;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
