package io.toxa108.blitzar.storage.database.manager.btree.impl;


import io.toxa108.blitzar.storage.database.schema.Key;

import java.util.Arrays;
import java.util.Objects;

public class TreeNode {
    /**
     * Keys
     */
    Key[] keys;

    /**
     * Pointers to childs(arrays of file seeks)
     */
    int[] p;

    /**
     * Entry values
     */
    byte[][] values;

    /**
     * Leaf or not
     */
    boolean leaf;

    /**
     * Number of entries in the node
     */
    int q;

    /**
     * Pointer to the next leaf (file seek)
     */
    int nextPos;

    /**
     * Current position (file seek)
     */
    int pos;

    public TreeNode(final int q, final int dataLen) {
        this.keys = new Key[q];
        this.p = new int[q + 1];
        this.leaf = true;
        this.q = 0;
        this.nextPos = -1;
        this.pos = -1;
        this.values = new byte[q][dataLen];
        for (int i = 0; i < q; i++) {
            values[i] = new byte[dataLen];
        }
    }

    public TreeNode(final int pos,
                    final Key[] keys,
                    final int[] p,
                    final boolean isLeaf,
                    final int q,
                    final int nextPos) {
        this.keys = keys;
        this.p = p;
        this.leaf = isLeaf;
        this.q = q;
        this.nextPos = nextPos;
        this.values = new byte[0][0];
        this.pos = pos;
    }

    public TreeNode(final int pos,
                    final Key[] keys,
                    final byte[][] values,
                    final boolean isLeaf,
                    final int q,
                    final int nextPos) {
        this.keys = keys;
        this.values = values;
        this.leaf = isLeaf;
        this.q = q;
        this.nextPos = nextPos;
        this.p = new int[q + 1];
        this.pos = pos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TreeNode treeNode = (TreeNode) o;
        return leaf == treeNode.leaf &&
                pos == treeNode.pos &&
                q == treeNode.q &&
                nextPos == treeNode.nextPos &&
                Arrays.equals(keys, treeNode.keys) &&
                Arrays.equals(p, treeNode.p) &&
                Arrays.deepEquals(values, treeNode.values);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(leaf, q, nextPos);
        result = 31 * result + Arrays.hashCode(keys);
        result = 31 * result + Arrays.hashCode(p);
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }
}

