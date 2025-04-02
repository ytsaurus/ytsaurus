package tech.ytsaurus.core.common;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.TError;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.yson.YsonError;
import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.yson.YsonTextWriter;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ytree.TAttribute;
import tech.ytsaurus.ytree.TAttributeDictionary;


@NonNullApi
public class YTsaurusError extends RuntimeException {
    private final TError error;

    public YTsaurusError(TError error) {
        super(createFullErrorDescription(error));
        this.error = error;
    }

    public static YTsaurusError parseFrom(Map<String, YTreeNode> error) {
        int code = Optional.ofNullable(error.get("code"))
                .map(YTreeNode::intValue)
                .orElse(0);

        String message = Optional.ofNullable(error.get("message"))
                .map(YTreeNode::stringValue)
                .orElse("");

        List<TAttribute> attributes = Optional.ofNullable(error.get("attributes"))
                .map(YTreeNode::asMap)
                .map(Map::entrySet)
                .map(entrySet -> entrySet.stream()
                        .map(entry -> TAttribute.newBuilder()
                                .setKey(entry.getKey())
                                .setValue(ByteString.copyFrom(entry.getValue().toBinary()))
                                .build()
                        )
                        .collect(Collectors.toUnmodifiableList())
                )
                .orElse(Collections.emptyList());

        List<TError> innerErrors = Optional.ofNullable(error.get("inner_errors"))
                .map(YTreeNode::asList)
                .map(list -> list.stream()
                        .map(YTreeNode::asMap)
                        .map(YTsaurusError::parseFrom)
                        .map(YTsaurusError::getError)
                        .collect(Collectors.toUnmodifiableList())
                )
                .orElse(Collections.emptyList());

        return new YTsaurusError(
                TError.newBuilder()
                        .setCode(code)
                        .setMessage(message)
                        .setAttributes(TAttributeDictionary.newBuilder()
                                .addAllAttributes(attributes)
                                .build()
                        )
                        .addAllInnerErrors(innerErrors)
                        .build()
        );
    }

    public TError getError() {
        return error;
    }

    /**
     * Check if error or one of inner error satisfy given predicate.
     */
    public boolean matches(Predicate<Integer> predicate) {
        return findMatching(error, e -> predicate.test(e.getCode())) != null;
    }

    /**
     * Check if error or one of inner error has specified code.
     */
    public boolean matches(int code) {
        return findMatchingError(code) != null;
    }

    /**
     * Returns error of one of the inner error which has specified code.
     * Returns null if no such error is found.
     */
    @Nullable
    public TError findMatchingError(int code) {
        return findMatching(error, e -> e.getCode() == code);
    }

    /**
     * Get error code of this error and all inner errors.
     */
    public Set<Integer> getErrorCodes() {
        Set<Integer> result = new HashSet<>();
        Consumer<TError> walker = new Consumer<>() {
            @Override
            public void accept(TError e) {
                result.add(e.getCode());
                for (TError inner : e.getInnerErrorsList()) {
                    this.accept(inner);
                }
            }
        };
        walker.accept(error);
        return result;
    }

    /**
     * Prefer to use  {@link #findMatchingError(int)}.
     */
    @Nullable
    @Deprecated
    public YTsaurusError findMatching(int code) {
        if (error.getCode() == code) {
            return this;
        }
        TError matching = findMatching(error, e -> e.getCode() == code);
        if (matching == null) {
            return null;
        }
        return new YTsaurusError(matching);
    }

    public boolean isUnrecoverable() {
        int code = error.getCode();
        return code == YTsaurusErrorCode.ProxyBanned.code || // COMPAT(babenko): drop ProxyBanned in favor of PeerBanned
                code == YTsaurusErrorCode.PeerBanned.code ||
                code == YTsaurusErrorCode.TransportError.code ||
                code == YTsaurusErrorCode.Unavailable.code ||
                code == YTsaurusErrorCode.NoSuchService.code ||
                code == YTsaurusErrorCode.NoSuchMethod.code ||
                code == YTsaurusErrorCode.ProtocolError.code;
    }

    public static boolean isUnrecoverable(Throwable e) {
        return !(e instanceof YTsaurusError) || ((YTsaurusError) e).isUnrecoverable();
    }

    public static String createFullErrorDescription(TError error) {
        StringBuilder sb = new StringBuilder();
        writeShortErrorDescription(error, sb);
        sb.append("; full error: ");
        try {
            serializeError(error, new YsonTextWriter(sb));
        } catch (YsonError ex) {
            sb.append("<yson parsing error occurred>");
        }
        return sb.toString();
    }

    @Nullable
    private static TError findMatching(@Nullable TError error, Predicate<TError> predicate) {
        if (error == null) {
            return null;
        } else if (predicate.test(error)) {
            return error;
        }

        TError result = null;
        for (TError inner : error.getInnerErrorsList()) {
            result = findMatching(inner, predicate);
            if (result != null) {
                break;
            }
        }
        return result;
    }

    private static void writeShortErrorDescription(TError error, StringBuilder sb) {
        while (true) {
            sb.append('\'').append(error.getMessage()).append('\'');
            if (error.getInnerErrorsCount() > 0) {
                sb.append(" <== ");
                // Usually we have no more than one inner error.
                // If there are many of them we output only the first one in short message description to ease reading.
                // All other inner errors will also be printed in "full error:" section.
                error = error.getInnerErrors(0);
            } else {
                break;
            }
        }
    }

    private static void serializeError(TError error, YsonConsumer consumer) {
        consumer.onBeginMap();

        consumer.onKeyedItem("code");
        consumer.onInteger(error.getCode());

        consumer.onKeyedItem("message");
        consumer.onString(error.getMessage());

        if (error.getAttributes().getAttributesCount() != 0) {
            consumer.onKeyedItem("attributes");
            consumer.onBeginMap();
            for (TAttribute attribute : error.getAttributes().getAttributesList()) {
                consumer.onKeyedItem(attribute.getKey());
                new YsonParser(attribute.getValue().toByteArray()).parseNode(consumer);
            }
            consumer.onEndMap();
        }

        if (error.getInnerErrorsCount() > 0) {
            consumer.onKeyedItem("inner_errors");
            consumer.onBeginList();
            for (TError innerError : error.getInnerErrorsList()) {
                serializeError(innerError, consumer);
            }
            consumer.onEndList();
        }
        consumer.onEndMap();
    }
}
