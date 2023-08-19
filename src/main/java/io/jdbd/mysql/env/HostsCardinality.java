package io.jdbd.mysql.env;

/**
 * The rules describing the number of hosts a database URL may contain.
 *
 * @since 1.0
 */
public enum HostsCardinality {

    SINGLE {
        @Override
        public boolean assertSize(int n) {
            return n == 1;
        }
    },
    MULTIPLE {
        @Override
        public boolean assertSize(int n) {
            return n > 1;
        }
    },
    ONE_OR_MORE {
        @Override
        public boolean assertSize(int n) {
            return n >= 1;
        }
    };

    public abstract boolean assertSize(int n);


}
