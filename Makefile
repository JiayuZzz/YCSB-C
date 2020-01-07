CC=g++
CFLAGS=-std=c++11 -g -Wall -pthread -I./
LDFLAGS=/usr/local/lib/librocksdb.a -lpthread -ltbb -llz4 -lbz2 -lzstd -lhiredis -lpebblesdb -lsnappy -lz -ldl
SUBDIRS=core db redis
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

