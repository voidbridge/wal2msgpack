MODULES = wal2msgpack
EXTENSION = wal2msgpack
DOCS = README.isbn_issn

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
