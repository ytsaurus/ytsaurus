#pragma once

#include <util/generic/vector.h>
#include <util/generic/ptr.h>

#include <mapreduce/yt/interface/io.h>

#include "proxy_output.h"
#include "node_table_writer.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeYdlTableWriter
    : public IYdlWriterImpl
{
public:
    TNodeYdlTableWriter(THolder<TProxyOutput> output, TVector<ui64> hashes);

    void AddRow(const TNode& row, size_t tableIndex) override;
    void VerifyRowType(ui64 rowTypeHash, size_t tableIndex) const override;

    size_t GetStreamCount() const override;
    IOutputStream* GetStream(size_t tableIndex) const override;
    void Abort() override;

private:
    THolder<TNodeTableWriter> NodeWriter_;
    TVector<ui64> TypeHashes_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
