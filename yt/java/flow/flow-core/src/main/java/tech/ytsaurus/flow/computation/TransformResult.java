package tech.ytsaurus.flow.computation;

import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.NewTimer;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Represents the result of a transformation operation, containing a list of produced messages, timers
 * and their associated parent IDs.
 */
public class TransformResult implements YTreeConvertible {
    private final List<String> parentIds;
    private @Nullable List<Message> messages;
    private @Nullable List<Boolean> distribute;
    private @Nullable List<NewTimer> timers;

    /**
     * Creates a new TransformResult with the given parent IDs.
     *
     * @param parentIds A list of parent IDs.
     */
    public TransformResult(List<String> parentIds) {
        this.parentIds = parentIds;
    }

    /**
     * Adds a message to the result.
     *
     * @param message The message to be added.
     */
    public void addMessage(Message message) {
        addMessage(message, true);
    }

    /**
     * Adds a message to the result with an explicit distribute flag.
     *
     * @param message    The message to be added.
     * @param distribute Whether the message should be published downstream.
     */
    public void addMessage(Message message, boolean distribute) {
        if (messages == null) {
            messages = new ArrayList<>(1);
            this.distribute = new ArrayList<>(1);
        }
        messages.add(message);
        this.distribute.add(distribute);
    }

    /**
     * Adds a list of messages to the result.
     *
     * @param messages The list of messages to be added.
     */
    public void addMessages(List<Message> messages) {
        if (messages.isEmpty()) {
            return;
        }
        if (this.messages == null) {
            this.messages = new ArrayList<>(messages.size());
            this.distribute = new ArrayList<>(messages.size());
        }
        this.messages.addAll(messages);
        for (int i = 0; i < messages.size(); ++i) {
            this.distribute.add(true);
        }
    }

    /**
     * Adds a timer to the result.
     *
     * @param timer The timer to be added.
     */
    public void addTimer(NewTimer timer) {
        if (timers == null) {
            timers = new ArrayList<>(1);
        }
        timers.add(timer);
    }

    public List<Message> getMessages() {
        return messages == null ? List.of() : messages;
    }

    public List<Boolean> getDistribute() {
        return distribute == null ? List.of() : distribute;
    }

    public List<NewTimer> getTimers() {
        return timers == null ? List.of() : timers;
    }

    public List<String> getParentIds() {
        return parentIds;
    }

    boolean isEmpty() {
        return (messages == null || messages.isEmpty())
                && (timers == null || timers.isEmpty());
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public YTreeNode toYTree() {
        var builder = YTree.builder().beginMap();
        builder.key("messages").beginList();
        for (var message : getMessages()) {
            builder.value(message.toYTree());
        }
        builder.endList();
        builder.key("timers").beginList();
        for (var timer : getTimers()) {
            builder.value(timer.toYTree());
        }
        builder.endList();
        builder.key("parent_ids").beginList();
        for (var parentId : parentIds) {
            builder.value(parentId);
        }
        builder.endList();
        return builder.endMap().build();
    }
}
