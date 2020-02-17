package io.toxa108.blitzar.storage.database.manager.storage.btree.impl;

import io.toxa108.blitzar.storage.database.schema.Key;

public class SearchKeys {

    /**
     * Binary search key in node
     *
     * @param keys       keys
     * @param keysLength len of keys
     * @param key        key
     * @return position of properly key
     */
    int searchKeyInNode(final Key[] keys, final int keysLength, final Key key) {
        int l = 0, r = keysLength, m;
        while (l < r) {
            m = (l + r) >>> 1;
            int c = keys[m].compareTo(key);
            if (c > 0) {
                r = m;
            } else if (c < 0) {
                l = m + 1;
            } else {
                return m;
            }
        }
        return -1;
    }

    /**
     * Binary search key in node
     *
     * @param keys       keys
     * @param keysLength len of keys
     * @param key        key
     * @return position of properly key
     */
    int searchTraverseWay(final Key[] keys, final int keysLength, final Key key) {
        int l = 0, r = keysLength, m;
        while (l < r) {
            m = (l + r) >>> 1;
            int c = keys[m].compareTo(key);
            if (c > 0) {
                r = m;
            } else if (c < 0) {
                l = m + 1;
            } else {
                return m;
            }
        }
        return l;
    }

    /**
     * Find properly position in node
     *
     * @param keys       keys
     * @param keysLength len of keys
     * @param key        key
     * @return position for insert
     */
    int findProperlyPosition(final Key[] keys, final int keysLength, final Key key) {
        int l = 0, r = keysLength, m;
        while (l < r) {
            m = (l + r) >>> 1;
            int c = keys[m].compareTo(key);
            if (c > 0) {
                r = m;
            } else if (c < 0) {
                l = m + 1;
            } else {
                return -1;
            }
        }
        return l;
    }
}
