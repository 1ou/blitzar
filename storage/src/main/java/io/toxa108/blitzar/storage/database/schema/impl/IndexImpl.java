package io.toxa108.blitzar.storage.database.schema.impl;

import io.toxa108.blitzar.storage.database.schema.Index;
import io.toxa108.blitzar.storage.io.BytesManipulator;
import io.toxa108.blitzar.storage.io.impl.BytesManipulatorImpl;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IndexImpl implements Index {
    private final Set<String> fields;
    private final IndexType type;
    private final BytesManipulator bytesManipulator;

    public IndexImpl(byte[] bytes) {
        this.bytesManipulator = new BytesManipulatorImpl();

        byte[] sizeBytes = new byte[Integer.BYTES];
        byte[] indexTypeBytes = new byte[Short.BYTES];
        System.arraycopy(bytes, 0, sizeBytes, 0, Integer.BYTES);
        System.arraycopy(bytes, 0, indexTypeBytes, Integer.BYTES, Short.BYTES);

        int size = bytesManipulator.bytesToInt(sizeBytes);
        type = IndexType.fromId(bytesManipulator.bytesToShort(indexTypeBytes));

        byte[] fieldsBytes = new byte[size];
        System.arraycopy(bytes, 0, fieldsBytes, Integer.BYTES + Short.BYTES, size);

        fields = Stream.of(new String(fieldsBytes).split("%"))
                .collect(Collectors.toSet());
    }

    public IndexImpl(Set<String> fields, IndexType type) {
        this.fields = fields;
        this.type = type;
        this.bytesManipulator = new BytesManipulatorImpl();
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
        int size = Short.BYTES +
                fields.stream()
                        .map(it -> it.length() + 1)
                        .reduce(Integer::sum)
                        .orElse(0);

        byte[] sizeBytes = bytesManipulator.intToBytes(size);
        byte[] typeBytes = bytesManipulator.shortToBytes(type.id());
        byte[] fieldsBytes = fields.stream()
                .map(it -> (it + "%").getBytes())
                .reduce((a, b) -> {
                    byte[] c = new byte[a.length + b.length];
                    System.arraycopy(a, 0, c, 0, a.length);
                    System.arraycopy(b, 0, c, a.length, b.length);
                    return c;
                }).orElse(new byte[0]);

        byte[] resultBytes = new byte[sizeBytes.length + typeBytes.length + fieldsBytes.length];
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
        IndexImpl index = (IndexImpl) o;
        return Objects.equals(fields, index.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }
}
