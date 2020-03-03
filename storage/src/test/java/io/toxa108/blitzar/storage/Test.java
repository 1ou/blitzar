package io.toxa108.blitzar.storage;

import java.util.*;

public class Test {

    @org.junit.jupiter.api.Test
    public void tt() {
        int N = 553;
        String s = String.valueOf(N);
        SortedMap<String, Integer> map = new TreeMap<>(Comparator.reverseOrder());
        for (int i = 0; i < s.length(); ++i) {
            map.merge(String.valueOf(s.charAt(i)), 1, (p, n) -> p += 1);
        }
        int result = 0, u = s.length() - 1;
        for (Map.Entry<String, Integer> k : map.entrySet()) {
            System.out.println(k.getKey() + " " + k.getValue());
            for (int y = 0; y < k.getValue(); y++) {
                result += Math.pow(10, u--) * Integer.parseInt(k.getKey());
            }
        }

        System.out.println("ANSWER " + result);
    }

    @org.junit.jupiter.api.Test
    public void tt2() {
        int[] x = {1, 8, 7, 3, 4, 1, 8};
        int[] y = {6, 4, 1, 8, 5, 1, 7};
//        int[] x = {6, 10, 1, 4, 3};
//        int[] y = {2, 5, 3, 1, 6};

        SortedMap<Integer, Object> map = new TreeMap<>(Comparator.naturalOrder());
        for (int i = 0; i < x.length; ++i) {
            map.put(x[i], new Object());
        }
        int max = 0, prev = -1;
        for (Map.Entry<Integer, Object> k : map.entrySet()) {
            if (prev == -1) {
                prev = k.getKey();
                continue;
            }

            int t = k.getKey() - prev;
            if (t > max) {
                max = t;
            }
            prev = k.getKey();
        }
        System.out.println("ANSWER " + max);
    }

    @org.junit.jupiter.api.Test
    public void tt3() {
        Random random = new Random();
        long t1 = System.currentTimeMillis();
        long k = 0;
        int o = 5;
        int[] t = new int[new Double(Math.pow(10, o)).intValue()];
        for (int i = 0; i < Math.pow(10, o); i++) {
            t[i] = random.nextInt(100);
        }
        for (int i = 0; i < Math.pow(10, o); i++) {
            for (int j = 0; j < Math.pow(10, o); j++) {
                int u = t[i] + t[j];
                k++;
            }
        }
        long t2 = System.currentTimeMillis();
        System.out.println("ANSWER " + k + "  " + (t2 - t1));
    }
}
