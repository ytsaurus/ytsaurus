#include "node_table_writer.h"

#include "proxy_output.h"

#include <library/yson/writer.h>
#include <mapreduce/yt/common/node_visitor.h>
#include <mapreduce/yt/common/log.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNodeTableWriter::TNodeTableWriter(THolder<TProxyOutput> output, EYsonFormat format)
    : Output_(std::move(output))
{
    for (size_t i = 0; i < Output_->GetStreamCount(); ++i) {
        Writers_.push_back(
            new TYsonWriter(Output_->GetStream(i), format, YT_LIST_FRAGMENT));
    }
}

TNodeTableWriter::~TNodeTableWriter()
{ }

size_t TNodeTableWriter::GetStreamCount() const
{
    return Output_->GetStreamCount();
}

IOutputStream* TNodeTableWriter::GetStream(size_t tableIndex) const
{
    return Output_->GetStream(tableIndex);
}

void TNodeTableWriter::AddRow(const TNode& row, size_t tableIndex)
{
    if (row.HasAttributes()) {
        ythrow TIOException() << "Row cannot have attributes";
    }

    static const TNode emptyMap = TNode::CreateMap();
    const TNode* outRow = &emptyMap;
    if (row.GetType() != TNode::UNDEFINED) {
        if (!row.IsMap()) {
            ythrow TIOException() << "Row should be a map node";
        } else {
            outRow = &row;
        }
    }

    auto* writer = Writers_[tableIndex].Get();
    writer->OnListItem();

    TNodeVisitor visitor(writer);
    visitor.Visit(*outRow);

    Output_->OnRowFinished(tableIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
