package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.manager.transaction.BzTableTableLocks;
import io.toxa108.blitzar.storage.database.manager.transaction.TableLocks;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Future;

public class BzTableTableLocksTest {
    private final Logger log = LoggerFactory.getLogger(BzTableTableLocksTest.class);

    private final TableLocks tableLocks = new BzTableTableLocks();

    @Test
    public void test_exclusive_lock() {
        Runnable writeProcess = () -> {
            log.info("R1 START");
            tableLocks.exclusive(10);
            sleep(500);
            log.info("R1 END");
        };

        Collection<Future<String>> futuresReaders = new ArrayList<>(10);
        Runnable r2 = () -> {
            log.info("R2 START");
            tableLocks.exclusive(10);
            log.info("R2 END");
        };

        new Thread(writeProcess).start();
        sleep(100);
        new Thread(r2).start();
        sleep(1000);
    }

    private void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}