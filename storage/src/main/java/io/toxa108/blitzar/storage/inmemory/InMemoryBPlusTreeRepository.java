package io.toxa108.blitzar.storage.inmemory;

import io.toxa108.blitzar.storage.Repository;
import io.toxa108.blitzar.storage.entity.Result;

import java.util.*;
import java.util.stream.Collectors;

public class InMemoryBPlusTreeRepository<K extends Comparable<? super K>, V>
        implements Repository<K, V> {

    public static class BTreeNode<K, V> {
        BTreeNode(int q) {
            this.keys = new ArrayList<>(q);
            this.p = new ArrayList<>(q + 1);
            this.leaf = true;
            this.q = 0;
            this.pNext = null;
        }


        List<K> keys;
        List<InMemoryBPlusTreeRepository.BTreeNode<K, V>> p;
        V value;
        boolean leaf;
        int q;
        BTreeNode<K, V> pNext;

        boolean isEmpty() {
            return keys.isEmpty();
        }
    }

    private BTreeNode<K, V> rootNode;
    private final int p;
    private final int pLeaf;

    public InMemoryBPlusTreeRepository(int p, int pLeaf) {
        this.p = p;
        this.pLeaf = pLeaf;
        this.rootNode = new BTreeNode<>(p);
    }

    @Override
    public V add(K key, V value) {
        BTreeNode<K, V> n = rootNode;
        Stack<BTreeNode<K, V>> stack = new Stack<>();
        /*
            Search the properly node for insert and add parents to the stack
         */
        while (!n.leaf) {
            stack.push(n);
            int q = n.q;
            if (key.compareTo(n.keys.get(0)) < 0) {
                n = n.p.get(0);
            } else if (key.compareTo(n.keys.get(q - 1)) > 0) {
                n = n.p.get(q);
            } else {
                int fn = search(n.keys, key);
                n = n.p.get(fn);
            }
        }
        int properlyPosition = findProperlyPosition(n.keys, key);
        /*
            If record with such key already exists
         */
        if (properlyPosition == -1) {
            throw new IllegalArgumentException(
                    "key: " + String.valueOf(key) + " was inserted into node [" +
                    n.keys.stream().map(K::toString).collect(Collectors.joining(", ")) + "]");
        } else {
            BTreeNode<K, V> newNode = new BTreeNode<>(p);
            /*
                If node is not full, insert new node
             */
            if (n.q < this.p - 1) {
                n.keys.add(properlyPosition, key);
                n.p.add(properlyPosition, new BTreeNode<>(p));
                n.q++;
            }
            /*
                Split node before insert
             */
            else {
                BTreeNode<K, V> tmp = new BTreeNode<>(this.p + 1);
                tmp.keys.addAll(List.copyOf(n.keys));
                tmp.p.addAll(List.copyOf(n.p));

                tmp.keys.add(properlyPosition, key);
                tmp.p.add(properlyPosition, new BTreeNode<>(this.p));

                newNode.pNext = n.pNext;
                int j = (pLeaf + 1) >>> 1;

                n.keys = new ArrayList<>();
                n.p = new ArrayList<>();
                n.keys.addAll(List.copyOf(tmp.keys.subList(0, j + 1)));
                n.p.addAll(List.copyOf(tmp.p.subList(0, j + 1)));
                n.pNext = newNode;
                n.q = j + 1;

                newNode.keys.addAll(List.copyOf(tmp.keys.subList(j + 1, tmp.keys.size())));
                newNode.p.addAll(List.copyOf(tmp.p.subList(j + 1, tmp.p.size())));
                newNode.q = tmp.p.size() - (j + 1);
                key = tmp.keys.get(j);

                boolean finished = false;
                while (!finished) {
                    if (stack.isEmpty()) {
                        rootNode = new BTreeNode<>(this.p);
                        rootNode.keys.add(key);
                        rootNode.p.addAll(Arrays.asList(n, newNode));
                        rootNode.leaf = false;
                        rootNode.q = 1;
                        finished = true;
                    } else {
                        n = stack.pop();
                    /*
                        If internal node n is not full
                        parent node is not full - no split
                     */
                        if (n.keys.size() < this.p - 1) {
                            properlyPosition = findProperlyPosition(n.keys, key);
                            n.keys.add(properlyPosition, key);
                            n.p.add(properlyPosition + 1, newNode);
                            n.q++;
                            finished = true;
                        } else {
                            tmp = new BTreeNode<>(this.p + 1);
                            tmp.keys.addAll(List.copyOf(n.keys));
                            tmp.p.addAll(List.copyOf(n.p));

                            properlyPosition = findProperlyPosition(n.keys, key);
                            tmp.keys.add(properlyPosition, key);
                            tmp.p.add(properlyPosition + 1, newNode);

                            newNode = new BTreeNode<>(this.p);
                            j = (this.p + 1) >>> 1;

                            n.keys = new ArrayList<>();
                            n.p = new ArrayList<>();
                            n.keys.addAll(List.copyOf(tmp.keys.subList(0, j - 1)));
                            n.p.addAll(List.copyOf(tmp.p.subList(0, j)));
                            n.q = j - 1;
                            n.leaf = false;

                            newNode.keys.addAll(List.copyOf(tmp.keys.subList(j, tmp.keys.size())));
                            newNode.p.addAll(List.copyOf(tmp.p.subList(j, tmp.p.size())));
                            newNode.q = tmp.p.size() - (j + 1);
                            newNode.leaf = false;

                            key = tmp.keys.get(j - 1);
                        }
                    }
                }
            }
        }
        return null;
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
        }
        return l;
    }

    public BTreeNode<K, V> root() {
        return this.rootNode;
    }

    @Override
    public List<Result<K, V>> all() {
        return null;
    }

    @Override
    public Optional<V> findByKey(K key) {
        BTreeNode<K, V> n = rootNode;
        /*
            Search the properly node for insert and add parents to the stack
         */
        while (!n.leaf) {
            int q = n.q;
            if (key.compareTo(n.keys.get(0)) < 0) {
                n = n.p.get(0);
            } else if (key.compareTo(n.keys.get(q - 1)) > 0) {
                n = n.p.get(q);
            } else {
                int fn = search(n.keys, key);
                n = n.p.get(fn);
            }
        }
        try {
            int properlyPosition = search(n.keys, key);
            return Optional.of(n.p.get(properlyPosition).value);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    @Override
    public void removeAll() {
        this.rootNode = new BTreeNode<>(this.p);
    }
}
