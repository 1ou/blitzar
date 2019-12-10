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

    V add(List<BTreeNode<K, V>> leaf, K key, V value) {
        if (leaf.isEmpty()) {
            BTreeNode<K, V> newNode = new BTreeNode<>(key, value);
            leaf.add(newNode);
            size++;
            return newNode.value;
        }

        /*
         * The tree has only one leaf
         */
        if (size < M) {
            V v = insertInLeaf(leaf, new BTreeNode<>(key, value));
            size++;
            return v;
        }
        /*
         * Split the root leaf to leafs
         */
        else if (size == M) {
            List<BTreeNode<K, V>> topLeaf = split(leaf);
            BTreeNode<K, V> newNode = new BTreeNode<>(key, value);
            if (newNode.key.compareTo(topLeaf.get(0).getKey()) > 0) {
                insertInLeaf(topLeaf.get(0).getRightChilds(), newNode);
            } else {
                insertInLeaf(topLeaf.get(0).getLeftChilds(), newNode);
            }
            this.size++;
            this.rootLeaf = topLeaf;
            return newNode.value;
        }
        /*
         * We need go deeper
         */
        else {
            if (leaf.size() == M) {
                // split
                // add newLeaf to parent leaf... but i have no pointer to parent leaf.
                List<BTreeNode<K, V>> newLeaf = split(leaf);
                return add(newLeaf, key, value);
            }
            List<BTreeNode<K, V>> nextLeaf = traverse(leaf, key);
            if (nextLeaf.size() == 0) {
                // insert
                V v = insertInLeaf(leaf, new BTreeNode<>(key, value));
                this.size++;
                return v;
            } else {
                return add(nextLeaf, key, value);
            }
        }
    }

    /**
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

    List<BTreeNode<K, V>> traverse(List<BTreeNode<K, V>> leaf, K key) {
        int l = 0, r = leaf.size(), m;
        while (l < r) {
            m = (l + r) >>> 1;
            int c = leaf.get(m).key.compareTo(key);
            if (c > 0) {
                r = m;
            } else if (c < 0) {
                l = m + 1;
            } else {
                throw new IllegalArgumentException();
            }
            m = (r + l) / 2;
        }
        if (l == leaf.size()) {
            return leaf.get(l - 1).getRightChilds();
        } else {
            return leaf.get(l).getLeftChilds();
        }
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
