package Algorithms.iforest.utlis;

import java.util.Iterator;
import java.util.stream.LongStream;

public class RangeIterator implements Iterator<Long> {
    private final long start;
    private final long end;
    private long current;

    public RangeIterator(long start, long end) {
        this.start = start;
        this.end = end;
        this.current = start;
    }

    @Override
    public boolean hasNext() {
        return current < end;
    }

    @Override
    public Long next() {
        if (!hasNext()) {
            throw new IllegalStateException("No more elements");
        }
        long result = current;
        current++;
        return result;
    }

    public static void main(String[] args) {
        long start = 1;
        long end = 10;

        Iterator<Long> iterator = new RangeIterator(start, end);

        while (iterator.hasNext()) {
            long value = iterator.next();
            System.out.println(value);
        }
    }
}