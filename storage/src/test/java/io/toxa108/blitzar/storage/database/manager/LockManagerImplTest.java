package io.toxa108.blitzar.storage.database.manager;

import io.toxa108.blitzar.storage.database.manager.transaction.LockManager;
import io.toxa108.blitzar.storage.database.manager.transaction.LockManagerImpl;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Future;

public class LockManagerImplTest {

    private final LockManager lockManager = new LockManagerImpl();

    @Test
    public void test_exclusive_lock() {
        Runnable writeProcess = () -> {
            System.out.println("R1 START");
            lockManager.exclusive(10);
            sleep(500);
            System.out.println("R1 END");
        };

        Collection<Future<String>> futuresReaders = new ArrayList<>(10);
        Runnable r2 = () -> {
            System.out.println("R2 START");
            lockManager.exclusive(10);
            System.out.println("R2 END");
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