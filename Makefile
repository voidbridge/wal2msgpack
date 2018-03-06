MODULES = wal2msgpack
EXTENSION = wal2msgpack
DOCS = README.md

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
