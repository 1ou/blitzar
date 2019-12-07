package io.toxa108.blitzar.storage.impl;

import org.junit.Assert;
import org.junit.Test;

public class InMemoryBTreeRepositoryTest {

    @Test
    public void insert_to_leaf_than_insert_in_left_than_success() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> newNode =
                new InMemoryBTreeRepository.BTreeNode<>(1L, 30L);

        inMemoryBTreeRepository.insertInNode(node, newNode);
        Assert.assertEquals(node.left, newNode);
    }

    @Test
    public void insert_to_leaf_than_insert_in_right_than_success() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> newNode =
                new InMemoryBTreeRepository.BTreeNode<>(100L, 30L);

        inMemoryBTreeRepository.insertInNode(node, newNode);
        Assert.assertEquals(newNode.left, node);
    }

    @Test(expected = IllegalArgumentException.class)
    public void insert_to_leaf_than_insert_than_unique_error() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> newNode =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 30L);

        inMemoryBTreeRepository.insertInNode(node, newNode);
    }

    @Test(expected = IllegalArgumentException.class)
    public void insert_to_leaf_than_insert_when_unique_error() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node2 =
                new InMemoryBTreeRepository.BTreeNode<>(5L, 20L);

        inMemoryBTreeRepository.insertInNode(node, node2);

        InMemoryBTreeRepository.BTreeNode<Long, Long> newNode =
                new InMemoryBTreeRepository.BTreeNode<>(5L, 30L);

        inMemoryBTreeRepository.insertInNode(node, newNode);
        Assert.assertEquals(node.left, node2);
        Assert.assertEquals(node2.left, newNode);
    }

    @Test
    public void insert_to_leaf_than_insert_in_left_case_2_than_success() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node2 =
                new InMemoryBTreeRepository.BTreeNode<>(5L, 20L);

        inMemoryBTreeRepository.insertInNode(node, node2);

        InMemoryBTreeRepository.BTreeNode<Long, Long> newNode =
                new InMemoryBTreeRepository.BTreeNode<>(1L, 30L);

        inMemoryBTreeRepository.insertInNode(node, newNode);
        Assert.assertEquals(node.left, node2);
        Assert.assertEquals(node2.left, newNode);
    }
}