#pragma once

///
/// @file text_lenval.h
///
/// TextLenval is a format that often used to save dumps of logborker topics to use them in tests.
/// This library provides possibility to read this format in roren local pipelines.
///
/// Format has a following structure:
///   - File contains one or more *chunks*.
///   - Each *chunk* has following structure:
///     1. *payloadSize* -- text encoded integer (in decimal format)
///     2. new line character ('\n')
///     3. *payloadSize* bytes of data
///     4. one more new line character ('\n')

#include <yt/cpp/roren/interface/fwd.h>

#include <util/stream/input.h>
#include <util/generic/string.h>

#include <util/ysaveload.h>

namespace NBigRT {
    struct TMessageBatch;
}

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

/// @brief Options for fine control of reading text lenval data.
struct TReadTextLenvalOptions
{
    ///
    /// @brief How many messages should contain each emited TMessageBatch object.
    ///
    /// Each "text lenval" chunk corresponds to single logbroker message.
    /// `ReadTextLenval...` functions combines multiple such messages into single TMesageBatch object.
    ///
    /// Default value 2 is chosen because of this reasons:
    ///  - we want to emit batches containing more than single message (in real life batches rarely contain single message).
    ///  - we don't want large batch size because logbroker "text lenval" files usualy contain small number of chunks
    int MessageBatchSize = 2;

    Y_SAVELOAD_DEFINE(MessageBatchSize);
};

///
/// @brief Parse file or memory region containing "text lenval" encoded data and emit message batches.
///
/// @note This reader designed to be used with local pipelines in unit tests and it doesn't work with other pipelines.
///
/// @ref NRoren::MakeLocalPipeline
/// @{
TTransform<void, NBigRT::TMessageBatch> ReadTextLenvalFromFile(TString fileName, const TReadTextLenvalOptions& options = {});
TTransform<void, NBigRT::TMessageBatch> ReadTextLenvalFromMemory(TString data, const TReadTextLenvalOptions& options = {});
/// @}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
