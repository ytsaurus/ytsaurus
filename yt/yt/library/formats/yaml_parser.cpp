#include "yaml_parser.h"

#include "yaml_helpers.h"

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/coro_pipe.h>

#include <contrib/libs/yaml/include/yaml.h>

namespace NYT::NFormats {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

//! A helper class that takes care of the repeated parts of a YAML document that
//! are expressed as anchors and aliases. Under the hood, materializes a YSON
//! string for each anchor and emits it to the underlying consumer via
//! OnRaw when needed.
/*!
 *  Implementation notes:
 *  - Conforming to YAML 1.2, alias may refer only to a previously defined anchor.
 *  - Aliasing an anchor to an ancestor node is not supported as the resulting document
 *    cannot be represent as a finite YSON (even though some implementations with tree
 *    representations support that, e.g. PyYAML).
 *  - According to the YAML spec, alias may be "overridden" by a later definition.
 *    This feature is considered error-prone, will probably be removed in next
 *    versions of YAML spec (https://github.com/yaml/yaml-spec/pull/65) and is not
 *    supported by us.
 *  - Using an alias to a scalar anchor as a map key or anchoring a map key are not
 *    supported for the sake of simpler implementation (and are considered a weird thing
 *    to do by an author of this code).
 */
class TAnchorRecordingConsumer
    : public IYsonConsumer
{
public:
    explicit TAnchorRecordingConsumer(IYsonConsumer* underlyingConsumer)
        : UnderlyingConsumer_(underlyingConsumer)
    { }

    void OnStringScalar(TStringBuf value) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnStringScalar(value); });
        MaybeFinishRecording();
    }

    void OnInt64Scalar(i64 value) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnInt64Scalar(value); });
        MaybeFinishRecording();
    }

    void OnUint64Scalar(ui64 value) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnUint64Scalar(value); });
        MaybeFinishRecording();
    }

    void OnDoubleScalar(double value) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnDoubleScalar(value); });
        MaybeFinishRecording();
    }

    void OnBooleanScalar(bool value) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnBooleanScalar(value); });
        MaybeFinishRecording();
    }

    void OnEntity() override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnEntity(); });
        MaybeFinishRecording();
    }

    void OnBeginList() override
    {
        ++CurrentDepth_;
        ForAllConsumers([=] (auto* consumer) { consumer->OnBeginList(); });
    }

    void OnListItem() override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnListItem(); });
    }

    void OnEndList() override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnEndList(); });
        --CurrentDepth_;
        MaybeFinishRecording();
    }

    void OnBeginMap() override
    {
        ++CurrentDepth_;
        ForAllConsumers([] (auto* consumer) { consumer->OnBeginMap(); });
    }

    void OnKeyedItem(TStringBuf key) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnKeyedItem(key); });
    }

    void OnEndMap() override
    {
        ForAllConsumers([] (auto* consumer) { consumer->OnEndMap(); });
        --CurrentDepth_;
        MaybeFinishRecording();
    }

    void OnBeginAttributes() override
    {
        ++CurrentDepth_;
        ForAllConsumers([] (auto* consumer) { consumer->OnBeginAttributes(); });
    }

    void OnEndAttributes() override
    {
        ForAllConsumers([] (auto* consumer) { consumer->OnEndAttributes(); });
        --CurrentDepth_;
        // NB: do not call MaybeFinishRecording here, as we do not want to record only
        // attribute map part of the node.
    }

    void OnRaw(TStringBuf yson, EYsonType type) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnRaw(yson, type); });
    }

    void OnAnchor(const TString& anchor)
    {
        if (FinishedAnchors_.contains(anchor)) {
            THROW_ERROR_EXCEPTION("Anchor %Qv is already defined", anchor);
        }
        RecordingAnchors_.emplace_back(anchor, CurrentDepth_);
    }

    void OnAlias(const TString& alias)
    {
        auto it = FinishedAnchors_.find(alias);
        if (it == FinishedAnchors_.end()) {
            THROW_ERROR_EXCEPTION("Alias %Qv refers to an undefined anchor", alias);
        }
        OnRaw(it->second.AsStringBuf(), EYsonType::Node);
    }

private:
    IYsonConsumer* UnderlyingConsumer_;

    struct TRecordingAnchor
    {
        TString Anchor;
        TStringStream Stream;
        TBufferedBinaryYsonWriter Writer;
        int Depth;
        TRecordingAnchor(TString anchor, int depth)
            : Anchor(std::move(anchor))
            , Writer(&Stream)
            , Depth(depth)
        { }
    };
    //! A stack of all anchors currently being constructed.
    std::deque<TRecordingAnchor> RecordingAnchors_;

    //! A map containing YSON representations of anchors that have been finished.
    THashMap<TString, TYsonString> FinishedAnchors_;

    i64 CurrentDepth_ = 0;

    void ForAllConsumers(auto&& action)
    {
        action(UnderlyingConsumer_);
        for (auto& recordingAnchor : RecordingAnchors_) {
            action(&recordingAnchor.Writer);
        }
    }

    void MaybeFinishRecording()
    {
        if (!RecordingAnchors_.empty() && CurrentDepth_ == RecordingAnchors_.back().Depth) {
            auto& recordingAnchor = RecordingAnchors_.back();
            recordingAnchor.Writer.Flush();
            TYsonString yson(recordingAnchor.Stream.Str(), EYsonType::Node);
            FinishedAnchors_.emplace(std::move(recordingAnchor.Anchor), std::move(yson));
            RecordingAnchors_.pop_back();
        }
    }
};

class TYamlParser
{
public:
    TYamlParser(IInputStream* input, IYsonConsumer* consumer, TYamlFormatConfigPtr config, EYsonType ysonType)
        : Input_(input)
        , Consumer_(consumer)
        , Config_(config)
        , YsonType_(ysonType)
        , Parser_(new yaml_parser_t, yaml_parser_delete)
    {
        yaml_parser_initialize(Parser_.get());
        yaml_parser_set_input(Parser_.get(), &ReadHandler, this);
    }

    void Parse()
    {
        VisitStream();
    }

private:
    IInputStream* Input_;
    TAnchorRecordingConsumer Consumer_;
    TYamlFormatConfigPtr Config_;
    EYsonType YsonType_;

    using TParserPtr = std::unique_ptr<yaml_parser_t, decltype(&yaml_parser_delete)>;
    TParserPtr Parser_;

    //! C++ wrapper around libyaml event object that takes
    //! care of freeing the contents of the event upon destruction.
    struct TEvent
        : public yaml_event_t
        , public TNonCopyable
    {
        TEvent()
        {
            // Just in case if destructor is called before
            // the event is initialized.
            memset(this, 0, sizeof(*this));
        }
        ~TEvent()
        {
            yaml_event_delete(this);
        }
        EYamlEventType Type() const
        {
            return static_cast<EYamlEventType>(type);
        }
    };

    TError ReadError_;

    static int ReadHandler(void* data, unsigned char* buffer, size_t size, size_t* size_read)
    {
        auto* yamlParser = reinterpret_cast<TYamlParser*>(data);
        auto* input = yamlParser->Input_;

        try {
            // IInputStream is similar to yaml_read_handler_t interface
            // in EOF case: former returns 0 from Read(), and latter
            // expects handler to set size_read to 0 and return 1
            *size_read = input->Read(buffer, size);
            return 1;
        } catch (const std::exception& ex) {
            // We do not expect the read handler to be called after an error.
            YT_ASSERT(yamlParser->ReadError_.IsOK());
            yamlParser->ReadError_ = TError(ex);
            return 0;
        }
    }

    //! A wrapper around C-style libyaml API calls that return 0 on error which
    //! throws an exception in case of an error.
    int SafeInvoke(auto* method, auto... args)
    {
        int result = method(args...);
        if (result == 0) {
            ThrowError();
        }
        return result;
    }

    //! Throw an exception formed from the emitter state and possibly the exception
    //! caught in the last write handler call.
    void ThrowError()
    {
        // Unfortunately, libyaml may sometimes YAML_NO_ERROR. This may lead
        // to unclear exceptions during parsing.
        auto yamlErrorType = static_cast<EYamlErrorType>(Parser_->error);
        auto error = TError("YAML parser error: %v", Parser_->problem)
            << TErrorAttribute("yaml_error_type", yamlErrorType)
            << TErrorAttribute("problem_offset", Parser_->problem_offset)
            << TErrorAttribute("problem_value", Parser_->problem_value)
            << TErrorAttribute("problem_mark", Parser_->problem_mark);
        if (Parser_->context) {
            error <<= TErrorAttribute("context", Parser_->context);
            error <<= TErrorAttribute("context_mark", Parser_->context_mark);
        }
        if (!ReadError_.IsOK()) {
            error <<= ReadError_;
        }

        THROW_ERROR error;
    }

    void PullEvent(
        std::initializer_list<EYamlEventType> expectedTypes,
        TEvent* event)
    {
        SafeInvoke(yaml_parser_parse, Parser_.get(), event);
        for (const auto expectedType : expectedTypes) {
            if (event->Type() == expectedType) {
                return;
            }
        }
        // TODO(max42): stack and position!
        THROW_ERROR_EXCEPTION(
            "Unexpected event type %Qlv, expected one of %Qlv",
            event->Type(),
            std::vector(expectedTypes));
    }

    void VisitStream()
    {
        {
            TEvent event;
            PullEvent({EYamlEventType::StreamStart}, &event);
        }
        while (true) {
            TEvent event;
            PullEvent({EYamlEventType::DocumentStart, EYamlEventType::StreamEnd}, &event);
            if (event.Type() == EYamlEventType::StreamEnd) {
                break;
            }
            if (YsonType_ == EYsonType::ListFragment) {
                Consumer_.OnListItem();
            }
            VisitDocument();
        }
    }

    void VisitDocument()
    {
        {
            TEvent event;
            PullEvent(
                {
                    EYamlEventType::Scalar,
                    EYamlEventType::SequenceStart,
                    EYamlEventType::MappingStart,
                    EYamlEventType::Alias,
                },
                &event);
            VisitNode(&event);
        }
        {
            TEvent event;
            PullEvent({EYamlEventType::DocumentEnd}, &event);
        }
    }

    void VisitNode(const TEvent* event)
    {
        auto maybeOnAnchor = [&] (yaml_char_t* anchor) {
            if (anchor) {
                Consumer_.OnAnchor(reinterpret_cast<const char*>(anchor));
            }
        };
        switch (event->Type()) {
            case EYamlEventType::Scalar:
                maybeOnAnchor(event->data.scalar.anchor);
                VisitScalar(event);
                break;
            case EYamlEventType::SequenceStart:
                maybeOnAnchor(event->data.scalar.anchor);
                VisitSequence(event);
                break;
            case EYamlEventType::MappingStart:
                maybeOnAnchor(event->data.scalar.anchor);
                VisitMapping(/*isAttributes*/ false);
                break;
            case EYamlEventType::Alias:
                Consumer_.OnAlias(reinterpret_cast<const char*>(event->data.alias.anchor));
                break;
            default:
                YT_ABORT();
        }
    }

    void VisitScalar(const TEvent* event)
    {
        auto scalar = event->data.scalar;
        TStringBuf yamlValue(reinterpret_cast<const char*>(scalar.value), scalar.length);

        // According to YAML spec, there are two non-specific tags "!" and "?", and all other
        // tags are specific.
        //
        // If the tag is missing, parser should assign tag "!" to non-plain (quoted) scalars,
        // and "?" to plain scalars and collection nodes. For some reason, libyaml does not
        // do that for us.
        //
        // Then, "!"-tagged scalars should always be treated as strings, i.e. "!" -> YT string.
        //
        // Specific tags are either recognized by us, in which case we deduce a corresponding YT type,
        // or we assign a string type otherwise.
        //
        // For the "?"-tagged scalars we perform the type deduction based on the scalar value
        // (which is the most often case, as almost nobody uses type tags in YAML).
        //
        // Cf. https://yaml.org/spec/1.2.2/#332-resolved-tags
        TStringBuf tag;
        if (scalar.tag) {
            tag = TStringBuf(reinterpret_cast<const char*>(scalar.tag));
        } else if (scalar.style != YAML_PLAIN_SCALAR_STYLE) {
            tag = "!";
        } else {
            tag = "?";
        }

        EYamlScalarType yamlType;
        if (tag != "?") {
            yamlType = DeduceScalarTypeFromTag(tag);
        } else {
            yamlType = DeduceScalarTypeFromValue(yamlValue);
        }
        auto [ytType, nonStringScalar] = ParseScalarValue(yamlValue, yamlType);
        switch (ytType) {
            case ENodeType::String:
                Consumer_.OnStringScalar(yamlValue);
                break;
            case ENodeType::Int64:
                Consumer_.OnInt64Scalar(nonStringScalar.Int64);
                break;
            case ENodeType::Uint64:
                Consumer_.OnUint64Scalar(nonStringScalar.Uint64);
                break;
            case ENodeType::Double:
                Consumer_.OnDoubleScalar(nonStringScalar.Double);
                break;
            case ENodeType::Boolean:
                Consumer_.OnBooleanScalar(nonStringScalar.Boolean);
                break;
            case ENodeType::Entity:
                Consumer_.OnEntity();
                break;
            default:
                YT_ABORT();
        }
    }

    void VisitSequence(const TEvent* event)
    {
        // NB: YSON node with attributes is represented as a yt/attrnode-tagged YAML sequence,
        // so handle it as a special case.
        if (TStringBuf(reinterpret_cast<const char*>(event->data.mapping_start.tag)) == YTAttrNodeTag) {
            VisitNodeWithAttributes();
            return;
        }

        Consumer_.OnBeginList();
        while (true) {
            TEvent event;
            PullEvent(
                {
                    EYamlEventType::SequenceEnd,
                    EYamlEventType::SequenceStart,
                    EYamlEventType::MappingStart,
                    EYamlEventType::Scalar,
                    EYamlEventType::Alias
                },
                &event);
            if (event.Type() == EYamlEventType::SequenceEnd) {
                break;
            }
            Consumer_.OnListItem();
            VisitNode(&event);
        }
        Consumer_.OnEndList();
    }

    void VisitNodeWithAttributes()
    {
        {
            TEvent event;
            PullEvent({EYamlEventType::MappingStart}, &event);
            VisitMapping(/*isAttributes*/ true);
        }
        {
            TEvent event;
            PullEvent(
                {
                    EYamlEventType::Scalar,
                    EYamlEventType::SequenceStart,
                    EYamlEventType::MappingStart,
                    EYamlEventType::Alias,
                },
                &event);
            VisitNode(&event);
        }
        {
            TEvent event;
            PullEvent({EYamlEventType::SequenceEnd}, &event);
        }
    }

    void VisitMapping(bool isAttributes)
    {
        isAttributes ? Consumer_.OnBeginAttributes() : Consumer_.OnBeginMap();
        while (true) {
            {
                TEvent event;
                PullEvent(
                    {
                        EYamlEventType::MappingEnd,
                        EYamlEventType::Scalar,
                        // Yes, YAML is weird enough to support aliases as keys!
                        EYamlEventType::Alias,
                    },
                    &event);
                if (event.Type() == EYamlEventType::MappingEnd) {
                    break;
                } else if (event.Type() == EYamlEventType::Alias) {
                    THROW_ERROR_EXCEPTION("Using alias as a map key is not supported");
                } else {
                    if (event.data.scalar.anchor) {
                        THROW_ERROR_EXCEPTION("Putting anchors on map keys is not supported");
                    }
                    TStringBuf key(reinterpret_cast<const char*>(event.data.scalar.value), event.data.scalar.length);
                    Consumer_.OnKeyedItem(key);
                }
            }
            {
                TEvent event;
                PullEvent(
                    {
                        EYamlEventType::Scalar,
                        EYamlEventType::SequenceStart,
                        EYamlEventType::MappingStart,
                        EYamlEventType::Alias,
                    },
                    &event);
                VisitNode(&event);
            }
        }
        isAttributes ? Consumer_.OnEndAttributes() : Consumer_.OnEndMap();
    }
};

void ParseYaml(
    IInputStream* input,
    IYsonConsumer* consumer,
    TYamlFormatConfigPtr config,
    EYsonType ysonType)
{
    TYamlParser parser(input, consumer, config, ysonType);
    parser.Parse();
}

////////////////////////////////////////////////////////////////////////////////

class TYamlPushParser
    : public IParser
{
public:
    TYamlPushParser(
        TYamlFormatConfigPtr config,
        IYsonConsumer* consumer,
        EYsonType ysonType)
        : ParserCoroPipe_(
            BIND([=] (IZeroCopyInput* stream) {
                ParseYaml(stream, consumer, config, ysonType);
            }))
    { }

    void Read(TStringBuf data) override
    {
        if (!data.empty()) {
            ParserCoroPipe_.Feed(data);
        }
    }

    void Finish() override
    {
        ParserCoroPipe_.Finish();
    }

private:
    TCoroPipe ParserCoroPipe_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYaml(
    IYsonConsumer* consumer,
    TYamlFormatConfigPtr config,
    EYsonType ysonType)
{
    return std::make_unique<TYamlPushParser>(config, consumer, ysonType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
