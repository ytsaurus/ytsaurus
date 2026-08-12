# Word Count in {{product-name}} Flow (C++)

This is a simple introductory example in C++. The [pipeline]({{source-root}}/yt/yt/flow/examples/cpp/word_count) reads text messages from the input queue, splits them into words, and counts the occurrences of each word using an external state table.

[Source code]({{source-root}}/yt/yt/flow/examples/cpp/word_count)

Both units of user logic are written as [process functions](../../../../flow/cpp/process-functions.md) (subclasses of `IProcessFunction`). The function itself is lightweight and testable, and its execution mode is selected by the built-in `Computation` adapter in the spec via the `processing_function` field.

## Pipeline components

### TTextReadFunction

`TTextReadFunction` is a process function (`IProcessFunction`) that is executed by `TProcessFunctionSourceComputation` (the source adapter). It reads text messages from the input queue, splits the text into words (by whitespace characters), and for each word with a length of at least `min_word_length`, it generates a `TWordMessage` object in the `words` output stream. The `min_word_length` parameter is read in `Init` via `initContext->GetParameters<TTextReaderParameters>()` from the `processing_function_parameters` block of the spec.

Since the executing `Computation` is a source (`TSwiftOrderedSourceComputation`), the output messages are not stored in {{product-name}} — only the metadata required for deterministic operation is saved. For more details about computation types, see the [Computations](../../../../flow/concepts/computation.md) section.

### TWordCountFunction

`TWordCountFunction` is a process function (`IProcessFunction`) that is executed by `TProcessFunctionComputation` (the transform adapter). It uses `TSimpleExternalStateManager` to work with the [state](../../../../flow/concepts/glossary.md#state). For each input word, it:

- Gets the current counter value from the external state table.
- Increases the counter by 1.
- Writes the updated value back.

For more details about working with state, see the [State](../../../../flow/concepts/stateful.md) section.

## Message types

`TWordMessage` is a subclass of `TYsonMessage`. It contains a single `Word` field. It is registered via the `YT_FLOW_DEFINE_YSON_MESSAGE` macro.

## Pipeline structure

The pipeline consists of two adapter computations connected by the `words` stream:

1. **source** (input queue) → **TProcessFunctionSourceComputation** (`processing_function = TTextReadFunction`) → `words` stream.
2. `words` stream → **TProcessFunctionComputation** (`processing_function = TWordCountFunction`) → state table (word → count).

In the spec for the counter, you specify `group_by_schema` with a hash of the word and the word itself — to ensure correct [partitioning](../../../../flow/concepts/glossary.md#partition). The state parameters (`TSimpleExternalStateManager` and the table path) are declared in the `external_state_managers` section of the `Computation` spec.

## The main function

In `main`, you do the following:

1. `NYT::NFlow::Initialize(argc, argv)` — initialize the Flow library.
2. Register the functions via `YT_FLOW_DEFINE_PROCESS_FUNCTION(TTextReadFunction)` and `YT_FLOW_DEFINE_PROCESS_FUNCTION(TWordCountFunction)`.
3. `TSimpleSpecBuilder` — a builder for registering streams. You register the `words` stream with the `TWordMessage` message type via `RegisterStream<TWordMessage>("words")`.
4. `TSimpleRunnerProgram` — run the pipeline.

## Source code

### TTextReadFunction

{% code '/yt/yt/flow/examples/cpp/word_count/lib/word_count_functions.cpp' lang='cpp' lines='[BEGIN text_reader]-[END text_reader]' keep-indents %}

### TWordCountFunction

{% code '/yt/yt/flow/examples/cpp/word_count/lib/word_count_functions.cpp' lang='cpp' lines='[BEGIN word_counter]-[END word_counter]' keep-indents %}

