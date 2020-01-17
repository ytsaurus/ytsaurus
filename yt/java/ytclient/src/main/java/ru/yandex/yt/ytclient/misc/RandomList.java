package ru.yandex.yt.ytclient.misc;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

/**
 * @author Kirill Batalin (batalin@yandex-team.ru)
 */
public class RandomList<T> implements List<T> {

    private final Random random;
    private final List<T> data;

    public RandomList(final Random random, final List<T> data) {
        this.random = random;
        this.data = data;
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public boolean contains(final Object o) {
        return data.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T1> T1[] toArray(final T1[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(final T t) {
        return true;
    }

    @Override
    public boolean remove(final Object o) {
        return true;
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        return data.containsAll(c);
    }

    @Override
    public boolean addAll(final Collection<? extends T> c) {
        return true;
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends T> c) {
        return true;
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        return true;
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        return true;
    }

    @Override
    public void clear() {
    }

    @Override
    public T get(final int index) {
        return data.get(random.nextInt(data.size()));
    }

    @Override
    public T set(final int index, final T element) {
        return null;
    }

    @Override
    public void add(final int index, final T element) {
    }

    @Override
    public T remove(final int index) {
        return null;
    }

    @Override
    public int indexOf(final Object o) {
        return data.indexOf(o);
    }

    @Override
    public int lastIndexOf(final Object o) {
        return data.lastIndexOf(o);
    }


    @Override
    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException();
    }


    @Override
    public ListIterator<T> listIterator(final int index) {
        throw new UnsupportedOperationException();
    }


    @Override
    public List<T> subList(final int fromIndex, final int toIndex) {
        return this;
    }
}
