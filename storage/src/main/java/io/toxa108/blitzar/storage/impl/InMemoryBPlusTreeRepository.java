package io.toxa108.blitzar.storage.impl;

import io.toxa108.blitzar.storage.Repository;
import io.toxa108.blitzar.storage.entity.Result;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class InMemoryBPlusTreeRepository<K extends Comparable<K>, V>
        implements Repository<K, V> {

    public static class BTreeNode<K, V> {
        BTreeNode(int q) {
            this.keys = new ArrayList<>(q);
            this.childs = new ArrayList<>(q + 1);
            this.leaf = true;
            this.q = 0;
        }

        BTreeNode(K key, V value, int q) {
            this.value = value;
            this.keys = new ArrayList<>(q);
            this.childs = new ArrayList<>(q + 1);
            this.leaf = true;
            this.q = 1;
        }

        List<K> keys;
        List<InMemoryBPlusTreeRepository.BTreeNode<K, V>> childs;
        V value;
        boolean leaf;
        int q;
    }

    private final Stack<BTreeNode<K, V>> stack = new Stack<>();
    private BTreeNode<K, V> rootNode;
    private final int q;

    public InMemoryBPlusTreeRepository(int q) {
        this.q = q;
    }

    @Override
    public V add(K key, V value) {
        BTreeNode<K, V> n = rootNode;
        /*
        Search the properly node for insert and add parents to the stack
         */
        while (!n.leaf) {
            stack.push(n);
            int q = n.q;
            if (n.keys.get(0).compareTo(key) < 0) {
                n = n.childs.get(0);
            } else if (n.keys.get(q - 1).compareTo(key) > 0) {
                n = n.childs.get(q);
            } else {
                int fn = search(n.keys, key);
                n = n.childs.get(fn);
            }
        }
        int properlyPosition = findProperlyPosition(n.keys, key);
        /*
            If record with such key already exists
         */
        if (properlyPosition == -1) {
            throw new IllegalArgumentException();
        } else {
            BTreeNode<K, V> newNode = new BTreeNode<>(q);
            /*
                If node is not full, insert new node
             */
            if (n.q < this.q) {
                n.keys.add(properlyPosition, key);
                n.childs.add(properlyPosition, newNode);
            }
            /*
                Split node before insert
             */
            else {

            }
        }
    }

    int search(List<K> keys, K key) {
        int l = 0, r = keys.size(), m;
        while (l < r) {
            m = (l + r) >>> 1;
            int c = keys.get(m).compareTo(key);
            if (c > 0) {
                r = m;
            } else if (c < 0) {
                l = m + 1;
            } else {
                throw new IllegalArgumentException();
            }
            m = (r + l) / 2;
        }
        return l;
    }

    int findProperlyPosition(List<K> keys, K key) {
        int l = 0, r = keys.size(), m;
        while (l < r) {
            m = (l + r) >>> 1;
            int c = keys.get(m).compareTo(key);
            if (c > 0) {
                r = m;
            } else if (c < 0) {
                l = m + 1;
            } else {
                return -1;
            }
            m = (r + l) >>> 1;
        }
        return l;
    }

    @Override
    public List<Result<K, V>> all() {
        return null;
    }
}
