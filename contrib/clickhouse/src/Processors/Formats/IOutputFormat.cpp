#include <Processors/Formats/IOutputFormat.h>
#include <IO/WriteBuffer.h>


namespace DB
{

IOutputFormat::IOutputFormat(const Block & header_, WriteBuffer & out_)
    : IProcessor({header_, header_, header_}, {}), out(out_)
{
}

IOutputFormat::Status IOutputFormat::prepare()
{
    if (has_input)
        return Status::Ready;

    for (auto kind : {Main, Totals, Extremes})
    {
        auto & input = getPort(kind);

        if (kind != Main && !input.isConnected())
            continue;

        if (input.isFinished())
            continue;

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        current_chunk = input.pull(true);
        current_block_kind = kind;
        has_input = true;
        return Status::Ready;
    }

    finished = true;

    if (!finalized)
        return Status::Ready;

    return Status::Finished;
}

static Chunk prepareTotals(Chunk chunk)
{
    if (!chunk.hasRows())
        return {};

    if (chunk.getNumRows() > 1)
    {
        /// This may happen if something like ARRAY JOIN was executed on totals.
        /// Skip rows except the first one.
        auto columns = chunk.detachColumns();
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
    }

    return chunk;
}

void IOutputFormat::work()
{
    writePrefixIfNeeded();

    if (finished && !finalized)
    {
        if (rows_before_limit_counter && rows_before_limit_counter->hasAppliedStep())
            setRowsBeforeLimit(rows_before_limit_counter->get());
        if (rows_before_aggregation_counter && rows_before_aggregation_counter->hasAppliedStep())
            setRowsBeforeAggregation(rows_before_aggregation_counter->get());
        finalize();
        return;
    }

    switch (current_block_kind)
    {
        case Main:
            result_rows += current_chunk.getNumRows();
            result_bytes += current_chunk.allocatedBytes();
            consume(std::move(current_chunk));
            break;
        case Totals:
            writeSuffixIfNeeded();
            if (auto totals = prepareTotals(std::move(current_chunk)))
            {
                consumeTotals(std::move(totals));
                are_totals_written = true;
            }
            break;
        case Extremes:
            writeSuffixIfNeeded();
            consumeExtremes(std::move(current_chunk));
            break;
    }

    if (auto_flush)
        flush();

    has_input = false;
}

void IOutputFormat::flush()
{
    out.next();
}

void IOutputFormat::write(const Block & block)
{
    writePrefixIfNeeded();
    consume(Chunk(block.getColumns(), block.rows()));

    if (auto_flush)
        flush();
}

void IOutputFormat::finalize()
{
    if (finalized)
        return;
    writePrefixIfNeeded();
    writeSuffixIfNeeded();
    finalizeImpl();

    if (auto_flush)
        flush();

    finalizeBuffers();
    finalized = true;
}

}
