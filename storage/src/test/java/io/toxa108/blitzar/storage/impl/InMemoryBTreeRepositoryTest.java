package io.toxa108.blitzar.storage.impl;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class InMemoryBTreeRepositoryTest {

    @Test
    public void insert_to_leaf_than_insert_in_left_than_success() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> newNode =
                new InMemoryBTreeRepository.BTreeNode<>(1L, 30L);

        List<InMemoryBTreeRepository.BTreeNode<Long, Long>> leaf = new ArrayList<>();
        inMemoryBTreeRepository.insertInLeaf(leaf, node);
        inMemoryBTreeRepository.insertInLeaf(leaf, newNode);
        Assert.assertEquals(leaf.get(0), newNode);
        Assert.assertEquals(leaf.get(1), node);
    }

    @Test
    public void insert_to_leaf_than_insert_in_right_than_success() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> newNode =
                new InMemoryBTreeRepository.BTreeNode<>(100L, 30L);

        List<InMemoryBTreeRepository.BTreeNode<Long, Long>> leaf = new ArrayList<>();
        inMemoryBTreeRepository.insertInLeaf(leaf, node);
        inMemoryBTreeRepository.insertInLeaf(leaf, newNode);

        Assert.assertEquals(leaf.get(0), node);
        Assert.assertEquals(leaf.get(1), newNode);
    }

    @Test(expected = IllegalArgumentException.class)
    public void insert_to_leaf_than_insert_than_unique_error() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> newNode =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 30L);

        List<InMemoryBTreeRepository.BTreeNode<Long, Long>> leaf = new ArrayList<>();
        inMemoryBTreeRepository.insertInLeaf(leaf, node);
        inMemoryBTreeRepository.insertInLeaf(leaf, newNode);
    }

    @Test(expected = IllegalArgumentException.class)
    public void insert_to_leaf_than_insert_when_unique_error() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node2 =
                new InMemoryBTreeRepository.BTreeNode<>(5L, 20L);

        List<InMemoryBTreeRepository.BTreeNode<Long, Long>> leaf = new ArrayList<>();
        inMemoryBTreeRepository.insertInLeaf(leaf, node);
        inMemoryBTreeRepository.insertInLeaf(leaf, node2);

        InMemoryBTreeRepository.BTreeNode<Long, Long> newNode =
                new InMemoryBTreeRepository.BTreeNode<>(5L, 30L);

        inMemoryBTreeRepository.insertInLeaf(leaf, newNode);
    }

    @Test
    public void insert_to_leaf_than_insert_in_left_case_2_than_success() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node2 =
                new InMemoryBTreeRepository.BTreeNode<>(5L, 20L);

        List<InMemoryBTreeRepository.BTreeNode<Long, Long>> leaf = new ArrayList<>();
        inMemoryBTreeRepository.insertInLeaf(leaf, node);
        inMemoryBTreeRepository.insertInLeaf(leaf, node2);

        InMemoryBTreeRepository.BTreeNode<Long, Long> newNode =
                new InMemoryBTreeRepository.BTreeNode<>(1L, 30L);

        inMemoryBTreeRepository.insertInLeaf(leaf, newNode);

        Assert.assertEquals(leaf.get(0), newNode);
        Assert.assertEquals(leaf.get(1), node2);
        Assert.assertEquals(leaf.get(2), node);
    }

    @Test
    public void insert_to_leaf_than_complex_insert_than_success() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node2 =
                new InMemoryBTreeRepository.BTreeNode<>(5L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node3 =
                new InMemoryBTreeRepository.BTreeNode<>(7L, 30L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node4 =
                new InMemoryBTreeRepository.BTreeNode<>(1L, 30L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node5 =
                new InMemoryBTreeRepository.BTreeNode<>(11L, 30L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node6 =
                new InMemoryBTreeRepository.BTreeNode<>(2L, 30L);

        List<InMemoryBTreeRepository.BTreeNode<Long, Long>> leaf = new ArrayList<>();
        inMemoryBTreeRepository.insertInLeaf(leaf, node);
        inMemoryBTreeRepository.insertInLeaf(leaf, node2);
        inMemoryBTreeRepository.insertInLeaf(leaf, node3);
        inMemoryBTreeRepository.insertInLeaf(leaf, node4);
        inMemoryBTreeRepository.insertInLeaf(leaf, node5);
        inMemoryBTreeRepository.insertInLeaf(leaf, node6);

        Assert.assertEquals(leaf.get(0), node4);
        Assert.assertEquals(leaf.get(1), node6);
        Assert.assertEquals(leaf.get(2), node2);
        Assert.assertEquals(leaf.get(3), node3);
        Assert.assertEquals(leaf.get(4), node);
        Assert.assertEquals(leaf.get(5), node5);
    }

    @Test
    public void split_leaf_when_success() {
        InMemoryBTreeRepository<Long, Long> inMemoryBTreeRepository = new InMemoryBTreeRepository<>(5);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node =
                new InMemoryBTreeRepository.BTreeNode<>(10L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node2 =
                new InMemoryBTreeRepository.BTreeNode<>(5L, 20L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node3 =
                new InMemoryBTreeRepository.BTreeNode<>(7L, 30L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node4 =
                new InMemoryBTreeRepository.BTreeNode<>(1L, 30L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node5 =
                new InMemoryBTreeRepository.BTreeNode<>(11L, 30L);

        InMemoryBTreeRepository.BTreeNode<Long, Long> node6 =
                new InMemoryBTreeRepository.BTreeNode<>(2L, 30L);

        List<InMemoryBTreeRepository.BTreeNode<Long, Long>> leaf = new ArrayList<>();
        inMemoryBTreeRepository.insertInLeaf(leaf, node);
        inMemoryBTreeRepository.insertInLeaf(leaf, node2);
        inMemoryBTreeRepository.insertInLeaf(leaf, node3);
        inMemoryBTreeRepository.insertInLeaf(leaf, node4);
        inMemoryBTreeRepository.insertInLeaf(leaf, node5);
        inMemoryBTreeRepository.insertInLeaf(leaf, node6);

        List<InMemoryBTreeRepository.BTreeNode<Long, Long>> split = inMemoryBTreeRepository.split(leaf);
        Assert.assertEquals(node3, split.get(0));

        Assert.assertEquals(node4, node3.getLeftChilds().get(0));
        Assert.assertEquals(node6, node3.getLeftChilds().get(1));
        Assert.assertEquals(node2, node3.getLeftChilds().get(2));

        Assert.assertEquals(node, node3.getRightChilds().get(0));
        Assert.assertEquals(node5, node3.getRightChilds().get(1));
    }
}