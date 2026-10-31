package org.apache.curator.spring.boot;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class CuratorTest {
    private TestingServer server;
    private CuratorFramework client;

    @BeforeEach
    void startServer() throws Exception {
        server = new TestingServer();
        client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(10, 3));
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
    }

    @AfterEach
    void stopServer() throws Exception {
        if (client != null) client.close();
        if (server != null) server.close();
    }

    @Test
    void commitsCreateUpdateAndDeleteAsOneTransaction() throws Exception {
        client.create().forPath("/curator");
        CuratorOp create = client.transactionOp().create().forPath("/curator/item", "first".getBytes(StandardCharsets.UTF_8));
        CuratorOp update = client.transactionOp().setData().forPath("/curator/item", "second".getBytes(StandardCharsets.UTF_8));
        CuratorOp delete = client.transactionOp().delete().forPath("/curator/item");
        assertEquals(3, client.transaction().forOperations(create, update, delete).size());
        assertNull(client.checkExists().forPath("/curator/item"));
    }

    @Test
    void receivesNodeChangesThroughCuratorCache() throws Exception {
        byte[] first = "first".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second".getBytes(StandardCharsets.UTF_8);
        CountDownLatch created = new CountDownLatch(1);
        CountDownLatch changed = new CountDownLatch(1);
        AtomicReference<byte[]> changedData = new AtomicReference<byte[]>();
        try (CuratorCache cache = CuratorCache.build(client, "/watched")) {
            cache.listenable().addListener((type, oldData, data) -> {
                if (type == CuratorCacheListener.Type.NODE_CREATED
                        && data != null && Arrays.equals(first, data.getData())) created.countDown();
                if (type == CuratorCacheListener.Type.NODE_CHANGED) {
                    changedData.set(data.getData());
                    changed.countDown();
                }
            });
            cache.start();
            client.create().forPath("/watched", first);
            assertTrue(created.await(5, TimeUnit.SECONDS), "cache did not observe the initial node data");
            client.setData().forPath("/watched", second);
            assertTrue(changed.await(5, TimeUnit.SECONDS), "cache did not observe the node update");
            assertArrayEquals(second, changedData.get());
        }
    }
}
