package io.toxa108.blitzar.storage.database.manager.storage.btree.impl;

import io.toxa108.blitzar.storage.database.manager.storage.btree.DiskTreeReader;
import io.toxa108.blitzar.storage.database.manager.storage.btree.TableTreeMetadata;
import io.toxa108.blitzar.storage.io.DiskReader;
import io.toxa108.blitzar.storage.io.impl.DiskNioReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BzDiskTreeReader implements DiskTreeReader {
    private final Logger log = LoggerFactory.getLogger(BzDiskTreeReader.class);
    /**
     * Disk writer
     */
    private final DiskReader diskReader;

    /**
     * Table metadata
     */
    private final TableTreeMetadata tableTreeMetadata;


    public BzDiskTreeReader(final File file,
                            final TableTreeMetadata tableTreeMetadata) throws IOException {
        this.diskReader = new DiskNioReader(file);
        this.tableTreeMetadata = tableTreeMetadata;
    }

    @Override
    public TreeNode read(final int pos) throws IOException {
        log.info("Read from: " + pos);
        final byte[] bytes = diskReader.read(pos, tableTreeMetadata.databaseConfiguration().diskPageSize());

        return new BzBytesToTreeNode(bytes, pos, tableTreeMetadata).transform();
    }
}
