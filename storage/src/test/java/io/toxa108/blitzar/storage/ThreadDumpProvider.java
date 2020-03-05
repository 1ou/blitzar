package io.toxa108.blitzar.storage;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class ThreadDumpProvider {
    public static String get() {
        final StringBuilder threadInfoStr = new StringBuilder();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        // long ids[] = threadMXBean.findMonitorDeadlockedThreads();
        // ThreadInfo threadInfo[] = threadMXBean.getThreadInfo(ids);

        final ThreadInfo[] threadInfo = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        System.out.println(threadInfo.length);
        for (ThreadInfo info : threadInfo) {
            threadInfoStr.append('"');
            threadInfoStr.append(info.getThreadName());
            threadInfoStr.append("\" ");
            final Thread.State state = info.getThreadState();
            threadInfoStr.append("\n   java.lang.Thread.State: ");
            threadInfoStr.append(state);
            final StackTraceElement[] stackTraceElements = info.getStackTrace();
            for (final StackTraceElement stackTraceElement : stackTraceElements) {
                threadInfoStr.append("\n        at ");
                threadInfoStr.append(stackTraceElement);
            }
            threadInfoStr.append("\n\n");
        }
        return threadInfoStr.toString();
    }
}
