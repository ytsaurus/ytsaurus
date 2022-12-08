package tech.ytsaurus.client;

import java.util.HashMap;
import java.util.function.Consumer;

class SlidingWindow<T> {
    private final int maxSize;
    private final HashMap<Integer, T> window;
    private final Consumer<T> callback;
    private int nextSequenceNumber = 0;

    SlidingWindow(int maxSize, Consumer<T> callback) {
        this.maxSize = maxSize;
        this.callback = callback;
        this.window = new HashMap<>();
    }

    public void add(int sequenceNumber, T value) {
        if (sequenceNumber < nextSequenceNumber) {
            throw new IllegalArgumentException("Packet sequence number " + sequenceNumber + " is too small, " +
                    "must be >= " + nextSequenceNumber);
        }
        if (window.containsKey(sequenceNumber)) {
            throw new IllegalArgumentException("Packet with sequence number " + sequenceNumber + " is already queued");
        }
        if (window.size() >= maxSize) {
            throw new IllegalStateException("Packet window overflow, max size is " + maxSize);
        }

        window.put(sequenceNumber, value);

        while (window.containsKey(nextSequenceNumber)) {
            callback.accept(window.get(nextSequenceNumber));
            window.remove(nextSequenceNumber);
            nextSequenceNumber++;
        }
    }
}
