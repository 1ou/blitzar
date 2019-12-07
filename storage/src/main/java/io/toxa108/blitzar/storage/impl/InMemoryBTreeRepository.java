package io.toxa108.blitzar.storage.impl;

import io.toxa108.blitzar.storage.Repository;
import io.toxa108.blitzar.storage.entity.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * In BTree Implementation.
 * @param <K>
 * @param <V>
 */
public class InMemoryBTreeRepository<K extends Comparable<K>, V> implements Repository<K, V> {
    public static class BTreeNode<K, V> {

        BTreeNode(K key, V value) {
            this.key = key;
            this.value = value;
            this.leaf = true;
            this.left = null;
            this.childs = new ArrayList<>();
        }

        BTreeNode<K, V> left;
        List<BTreeNode<K, V>> childs;
        K key;
        V value;
        boolean leaf;
    }

    private BTreeNode<K, V> root;
    private int M; // degree of tree
    private int size = 0; // number of nodes

    public InMemoryBTreeRepository(int degree) {
        this.root = null;
        this.M = degree;
    }

    @Override
    public V add(K key, V value) {
        return add(root, key, value);
    }

    V add(BTreeNode<K, V> node, K key, V value) {
        if (node == null) {
            root = new BTreeNode<>(key, value);
            return root.value;
        }

        /*
         * The tree has only one leaf
         */
        if (size < M - 1) {
            return insertInNode(node, new BTreeNode<>(key, value));
        }
        /**
         * Split the root leaf to leafs
         */
        else {
            return null;
        }
    }

    /**
     * The complexity equals O(n), but we can use binary search here. Think later - how.
     * @param node node is always the rightest element.
     * @param newNode new node
     * @return inserted value
     * @throws IllegalArgumentException when uniqueness of tree is violated
     */
    V insertInNode(BTreeNode<K, V> node, BTreeNode<K, V> newNode) {
        if (node.key.compareTo(newNode.key) > 0) {
            int c = node.key.compareTo(newNode.key);
            while (node.left != null && c > 0) {
                node = node.left;
                c = node.key.compareTo(newNode.key);
            }
            if (c == 0) {
                throw new IllegalArgumentException();
            } else if (node.left == null) {
                node.left = newNode;
            }
            return newNode.value;
        } else if (node.key.compareTo(newNode.key) < 0) {
            newNode.left = node;
            return newNode.value;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public List<Result<K, V>> all() {
        return null;
    }
}
