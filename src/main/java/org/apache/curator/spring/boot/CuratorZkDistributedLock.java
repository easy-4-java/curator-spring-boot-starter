package org.apache.curator.spring.boot;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/** A per-thread handle for Curator's ephemeral, sequential ZooKeeper locks. */
public class CuratorZkDistributedLock implements Watcher {

    private final CuratorFramework curatorClient;
    private final int sessionTimeout;
    private final ThreadLocal<Deque<InterProcessMutex>> heldLocks = ThreadLocal.withInitial(ArrayDeque::new);

    public CuratorZkDistributedLock(CuratorFramework curatorClient, int sessionTimeout) {
        this.curatorClient = curatorClient;
        this.sessionTimeout = sessionTimeout;
    }

    public void acquireLock(String lockKey) {
        InterProcessMutex lock = new InterProcessMutex(curatorClient, "/locks/" + lockKey);
        try {
            if (!lock.acquire(sessionTimeout, TimeUnit.MILLISECONDS)) {
                throw new CuratorLockException("Timed out waiting for lock: " + lockKey);
            }
            heldLocks.get().push(lock);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CuratorLockException(e);
        } catch (CuratorLockException e) {
            throw e;
        } catch (Exception e) {
            throw new CuratorLockException(e);
        }
    }

    public boolean tryLock(String lockKey) throws Exception {
        InterProcessMutex lock = new InterProcessMutex(curatorClient, "/locks/" + lockKey);
        if (lock.acquire(0, TimeUnit.MILLISECONDS)) {
            heldLocks.get().push(lock);
            return true;
        }
        return false;
    }

    public boolean unlock() {
        Deque<InterProcessMutex> locks = heldLocks.get();
        if (locks.isEmpty()) {
            return false;
        }
        try {
            locks.peek().release();
            locks.pop();
            if (locks.isEmpty()) {
                heldLocks.remove();
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** Kept for callers that previously registered this instance as a watcher. */
    @Override
    public void process(WatchedEvent event) {
        // Curator's InterProcessMutex owns the predecessor-node watches.
    }
}
