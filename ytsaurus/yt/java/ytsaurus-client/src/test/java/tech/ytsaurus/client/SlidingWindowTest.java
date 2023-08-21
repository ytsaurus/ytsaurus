package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SlidingWindowTest {
    @Test
    public void testWindow() {
        List<Integer> list = new ArrayList<>(10);
        SlidingWindow<Integer> window = new SlidingWindow<>(10, list::add);
        List<Integer> indices = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        Random rnd = new Random(12345);
        Collections.shuffle(indices, rnd);

        for (int i : indices) {
            window.add(i, i);
        }

        assertEquals(list, List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    @Test
    public void testMaxSizeOk() {
        List<Integer> list = new ArrayList<>(10);
        SlidingWindow<Integer> window = new SlidingWindow<>(3, list::add);

        // deleting elements right after addition, window size is always 1
        for (int i = 0; i < 10; i++) {
            window.add(i, i);
        }

        assertEquals(list, List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    @Test(expected = IllegalStateException.class)
    public void testMaxSizeExceeded() {
        List<Integer> list = new ArrayList<>(10);
        SlidingWindow<Integer> window = new SlidingWindow<>(3, list::add);

        // keeping all elements in window, max size is exceeded
        for (int i = 9; i >= 0; i--) {
            window.add(i, i);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSequenceNumberRepeated() {
        List<Integer> list = new ArrayList<>(10);
        SlidingWindow<Integer> window = new SlidingWindow<>(3, list::add);

        window.add(1, 1);
        window.add(1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooSmallSequenceNumber() {
        List<Integer> list = new ArrayList<>(10);
        SlidingWindow<Integer> window = new SlidingWindow<>(3, list::add);

        window.add(0, 0);
        window.add(0, 0);
    }
}
