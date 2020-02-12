package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Index;
import io.toxa108.blitzar.storage.io.impl.BytesManipulator;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BzIndex implements Index {
    private final Set<String> fields;
    private final IndexType type;

    public BzIndex(final byte[] bytes) {
        final byte[] sizeBytes = new byte[Integer.BYTES];
        final byte[] indexTypeBytes = new byte[Short.BYTES];
        System.arraycopy(bytes, 0, sizeBytes, 0, Integer.BYTES);
        System.arraycopy(bytes, Integer.BYTES, indexTypeBytes, 0, Short.BYTES);

        final int size = BytesManipulator.bytesToInt(sizeBytes);
        this.type = IndexType.fromId(BytesManipulator.bytesToShort(indexTypeBytes));

        final int fieldsMetadataSize = size - Short.BYTES;
        final byte[] fieldsBytes = new byte[fieldsMetadataSize];
        System.arraycopy(bytes, Integer.BYTES + Short.BYTES, fieldsBytes, 0, fieldsMetadataSize);

        this.fields = Stream.of(new String(fieldsBytes).split("%"))
                .collect(Collectors.toSet());
    }

    public BzIndex(final Set<String> fields, final IndexType type) {
        this.fields = fields;
        this.type = type;
    }

    @Override
    public IndexType type() {
        return type;
    }

    @Override
    public Set<String> fields() {
        return fields;
    }

    @Override
    public byte[] toBytes() {
        final int size = Short.BYTES +
                fields.stream()
                        .map(it -> it.length() + 1)
                        .reduce(Integer::sum)
                        .orElse(0);

        final byte[] sizeBytes = BytesManipulator.intToBytes(size);
        final byte[] typeBytes = BytesManipulator.shortToBytes(type.id());
        final byte[] fieldsBytes = fields.stream()
                .map(it -> (it + "%").getBytes())
                .reduce((a, b) -> {
                    byte[] c = new byte[a.length + b.length];
                    System.arraycopy(a, 0, c, 0, a.length);
                    System.arraycopy(b, 0, c, a.length, b.length);
                    return c;
                }).orElse(new byte[0]);

        final byte[] resultBytes = new byte[sizeBytes.length + typeBytes.length + fieldsBytes.length];
        System.arraycopy(sizeBytes, 0, resultBytes, 0, sizeBytes.length);
        System.arraycopy(typeBytes, 0, resultBytes, sizeBytes.length, typeBytes.length);
        System.arraycopy(fieldsBytes, 0, resultBytes, sizeBytes.length + typeBytes.length, fieldsBytes.length);
        return resultBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BzIndex index = (BzIndex) o;
        return Objects.equals(fields, index.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }
}
