package io.toxa108.blitzar.storage.impl;

import io.toxa108.blitzar.storage.Repository;
import io.toxa108.blitzar.storage.entity.Result;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * In BTree Implementation.
 *
 * @param <K>
 * @param <V>
 * @author toxa
 */
public class InMemoryBTreeRepository<K extends Comparable<K>, V> implements Repository<K, V> {

    @Data
    public static class BTreeNode<K, V> {
        BTreeNode(K key, V value) {
            this.key = key;
            this.value = value;
            this.leftChilds = new ArrayList<>();
            this.rightChilds = new ArrayList<>();
        }

        List<BTreeNode<K, V>> leftChilds;
        List<BTreeNode<K, V>> rightChilds;
        K key;
        V value;
    }

    private List<BTreeNode<K, V>> rootLeaf;
    private int M;
    private int size = 0;
    private final int minLeafSize = 4;

    public InMemoryBTreeRepository(int degree) {
        this.rootLeaf = new ArrayList<>();
        this.M = degree;
    }

    @Override
    public V add(K key, V value) {
        return add(rootLeaf, key, value);
    }

    V add(List<BTreeNode<K, V>> rootLeaf, K key, V value) {
        if (rootLeaf.isEmpty()) {
            BTreeNode<K, V> newNode = new BTreeNode<>(key, value);
            rootLeaf.add(newNode);
            size++;
            return newNode.value;
        }

        /*
         * The tree has only one leaf
         */
        if (size < M - 1) {
            V v = insertInLeaf(rootLeaf, new BTreeNode<>(key, value));
            size++;
            return v;
        }
        /*
         * Split the root leaf to leafs
         */
        else {
            return null;
        }
    }

    /**
     * The complexity equals O(n), but we can use binary search here. Think later - how.
     *
     * @param leaf    leaf
     * @param newNode new node
     * @return inserting value
     * @throws IllegalArgumentException when uniqueness of tree is violated
     */
    V insertInLeaf(List<BTreeNode<K, V>> leaf, BTreeNode<K, V> newNode) {
        int l = 0, r = leaf.size(), m;
        while (l < r) {
            m = (l + r) >>> 1;
            int c = leaf.get(m).key.compareTo(newNode.key);
            if (c > 0) {
                r = m;
            } else if (c < 0) {
                l = m + 1;
            } else {
                throw new IllegalArgumentException();
            }
            m = (r + l) / 2;
        }
        leaf.add(l, newNode);
        return newNode.value;
    }

    /**
     * Spit leaf to several leafs
     *
     * @param leaf size of leaf must be more than 3
     * @throws IllegalArgumentException if {@param leaf} size is less than {minLeafSize}
     */
    List<BTreeNode<K, V>> split(List<BTreeNode<K, V>> leaf) {
        if (leaf.size() < minLeafSize) {
            throw new IllegalArgumentException();
        }
        int m = (leaf.size() >>> 1);
        BTreeNode<K, V> node = leaf.get(m);

        node.leftChilds.addAll(leaf.subList(0, m));
        node.rightChilds.addAll(leaf.subList(m + 1, leaf.size()));

        List<BTreeNode<K, V>> splitLeaf = new ArrayList<>();
        splitLeaf.add(node);
        return splitLeaf;
    }

    @Override
    public List<Result<K, V>> all() {
        return null;
    }
}
