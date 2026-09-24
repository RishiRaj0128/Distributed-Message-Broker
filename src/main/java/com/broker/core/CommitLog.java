package com.broker.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe append-only commit log for an individual topic partition.
 */
public class CommitLog {
    private final List<Message> entries = new ArrayList<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private long nextOffset = 0L;

    public Message append(Message message) {
        rwLock.writeLock().lock();
        try {
            long assignedOffset = nextOffset++;
            Message assigned = message.withOffset(assignedOffset);
            entries.add(assigned);
            return assigned;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public List<Message> appendBatch(List<Message> messages) {
        rwLock.writeLock().lock();
        try {
            List<Message> committed = new ArrayList<>(messages.size());
            for (Message m : messages) {
                long assignedOffset = nextOffset++;
                Message assigned = m.withOffset(assignedOffset);
                entries.add(assigned);
                committed.add(assigned);
            }
            return committed;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Reads messages starting from startOffset up to maxCount.
     */
    public List<Message> read(long startOffset, int maxCount) {
        rwLock.readLock().lock();
        try {
            if (startOffset < 0 || startOffset >= entries.size() || maxCount <= 0) {
                return Collections.emptyList();
            }
            int fromIndex = (int) startOffset;
            int toIndex = Math.min(entries.size(), fromIndex + maxCount);
            return Collections.unmodifiableList(new ArrayList<>(entries.subList(fromIndex, toIndex)));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public long getLogEndOffset() {
        rwLock.readLock().lock();
        try {
            return nextOffset;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int size() {
        rwLock.readLock().lock();
        try {
            return entries.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Truncates log to match leader up to specific offset during replica reconciliation.
     */
    public void truncateTo(long targetOffset) {
        rwLock.writeLock().lock();
        try {
            if (targetOffset < entries.size()) {
                entries.subList((int) targetOffset, entries.size()).clear();
                nextOffset = targetOffset;
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
