package io.jdbd.session;

import java.util.Objects;

final class TransactionOptionImpl implements TransactionOption {

    static TransactionOptionImpl option(Isolation isolation, boolean readOnly) {
        return new TransactionOptionImpl(isolation, readOnly);
    }

    private final Isolation isolation;

    private final boolean readOnly;

    private TransactionOptionImpl(Isolation isolation, boolean readOnly) {
        this.isolation = isolation;
        this.readOnly = readOnly;
    }

    @Override
    public Isolation getIsolation() {
        return this.isolation;
    }

    @Override
    public boolean isReadOnly() {
        return this.readOnly;
    }

    @Override
    public boolean inTransaction() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(false, this.isolation, this.readOnly);
    }

    @Override
    public boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof TransactionOption) {
            final TransactionOption option = (TransactionOption) obj;
            match = !option.inTransaction()
                    && option.getIsolation() == this.isolation
                    && option.isReadOnly() == this.readOnly;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public String toString() {
        return String.format("TransactionOption{inTransaction:false,isolation:%s,readOnly:%s}."
                , this.isolation, this.readOnly);
    }


}
