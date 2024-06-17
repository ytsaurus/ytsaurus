package tech.ytsaurus.client.request;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.rpcproxy.TQuery;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ytree.TAttribute;

/**
 * Immutable information about query.
 *
 * @see tech.ytsaurus.client.ApiServiceClient#getQuery(GetQuery)
 */
public class Query {
    private final GUID id;
    @Nullable
    private final QueryEngine engine;
    @Nullable
    private final String query;
    @Nullable
    private final YTreeNode files;
    @Nullable
    private final Instant startTime;
    @Nullable
    private final Instant finishTime;
    private final YTreeNode settings;
    @Nullable
    private final String user;
    @Nullable
    private final YTreeNode accessControlObjects;
    @Nullable
    private final QueryState state;
    @Nullable
    private final Long resultCount;
    private final YTreeNode progress;
    @Nullable
    private final YTsaurusError error;
    private final YTreeNode annotations;
    private final Map<String, YTreeNode> otherAttributes;

    public Query(TQuery query) {
        this.id = RpcUtil.fromProto(query.getId());
        this.engine = query.hasEngine() ? QueryEngine.fromProtoValue(query.getEngine()) : null;
        this.query = query.hasQuery() ? query.getQuery() : null;
        this.files = query.hasFiles() ? RpcUtil.parseByteString(query.getFiles()) : null;
        this.startTime = query.hasStartTime() ? RpcUtil.instantFromMicros(query.getStartTime()) : null;
        this.finishTime = query.hasFinishTime() ? RpcUtil.instantFromMicros(query.getFinishTime()) : null;
        this.settings = RpcUtil.parseByteString(query.getSettings());
        this.user = query.hasUser() ? query.getUser() : null;
        this.accessControlObjects = query.hasAccessControlObjects()
                ? RpcUtil.parseByteString(query.getAccessControlObjects())
                : null;
        this.state = query.hasState() ? QueryState.fromProtoValue(query.getState()) : null;
        this.resultCount = query.hasResultCount() ? query.getResultCount() : null;
        this.progress = RpcUtil.parseByteString(query.getProgress());
        this.error = query.hasError() ? new YTsaurusError(query.getError()) : null;
        this.annotations = RpcUtil.parseByteString(query.getAnnotations());
        this.otherAttributes = query.getOtherAttributes().getAttributesList().stream()
                .collect(Collectors.toMap(
                        TAttribute::getKey,
                        attribute -> RpcUtil.parseByteString(attribute.getValue())
                ));
    }

    /**
     * Get query id.
     *
     * @return query id.
     */
    public GUID getId() {
        return id;
    }

    /**
     * Get query engine if present.
     *
     * @return query engine if present.
     * @see QueryEngine
     */
    public Optional<QueryEngine> getEngine() {
        return Optional.ofNullable(engine);
    }

    /**
     * Get query text if present.
     *
     * @return query if present.
     */
    public Optional<String> getQuery() {
        return Optional.ofNullable(query);
    }

    /**
     * Get list of files for query if present.
     *
     * @return files if present.
     */
    public Optional<YTreeNode> getFiles() {
        return Optional.ofNullable(files);
    }

    /**
     * Get start time of query if present.
     *
     * @return start time if present.
     */
    public Optional<Instant> getStartTime() {
        return Optional.ofNullable(startTime);
    }

    /**
     * Get finish time of query if present.
     *
     * @return finish time if present.
     */
    public Optional<Instant> getFinishTime() {
        return Optional.ofNullable(finishTime);
    }

    /**
     * Get additional query parameters.
     *
     * @return settings.
     */
    public YTreeNode getSettings() {
        return settings;
    }

    /**
     * Get username if present.
     *
     * @return username if present.
     */
    public Optional<String> getUser() {
        return Optional.ofNullable(user);
    }

    /**
     * Get list of access control objects if present.
     *
     * @return access control objects if present.
     */
    public Optional<YTreeNode> getAccessControlObjects() {
        return Optional.ofNullable(accessControlObjects);
    }

    /**
     * Get query state if present.
     *
     * @return query state if present.
     * @see QueryState
     */
    public Optional<QueryState> getState() {
        return Optional.ofNullable(state);
    }

    /**
     * Get result count if present.
     *
     * @return result count if present.
     */
    public Optional<Long> getResultCount() {
        return Optional.ofNullable(resultCount);
    }

    /**
     * Get query execution progress.
     *
     * @return progress.
     */
    public YTreeNode getProgress() {
        return progress;
    }

    /**
     * Get result error if present.
     *
     * @return error if present.
     */
    public Optional<YTsaurusError> getError() {
        return Optional.ofNullable(error);
    }

    /**
     * Get custom annotations for the query.
     *
     * @return annotations.
     */
    public YTreeNode getAnnotations() {
        return annotations;
    }

    /**
     * Get other attributes.
     *
     * @return attributes.
     */
    public Map<String, YTreeNode> getOtherAttributes() {
        return otherAttributes;
    }
}
