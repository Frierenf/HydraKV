CC = /usr/bin/g++
YCSBC_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
ROOT := $(abspath $(YCSBC_DIR)/..)
BUILD_DIR ?= $(ROOT)/build-linux
LEVELDB_LIB ?= $(BUILD_DIR)/libleveldb.a

CPPFLAGS += -I"$(ROOT)/ycsbc" -I"$(ROOT)/include" -I"$(ROOT)"
CFLAGS = -std=c++17 -O3 -DNDEBUG -Wall -pthread
LDFLAGS = "$(LEVELDB_LIB)" -lz -lsnappy -lpmem -lpmemobj -lnuma -lpthread

EXEC = ycsbc

OBJECTS = \
	core/core_workload.o \
	db/db_factory.o \
	db/leveldb_db.o

all: $(EXEC)

$(EXEC): ycsbc.cc $(OBJECTS) $(LEVELDB_LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) ycsbc.cc $(OBJECTS) $(LDFLAGS) -o $@

core/%.o: core/%.cc
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

db/%.o: db/%.cc
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

clean:
	$(RM) $(OBJECTS) $(EXEC)

.PHONY: all clean
