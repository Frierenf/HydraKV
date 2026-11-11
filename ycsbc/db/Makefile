CC = g++
CFLAGS = -std=c++11 -O3 -DNDEBUG -Wall -fgnu-tm -I/home/user/wzq/HydraKV513-8/include

SOURCES = $(wildcard *.cc)
OBJECTS = $(SOURCES:.cc=.o)

all: $(OBJECTS)

%.o: %.cc
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	$(RM) $(OBJECTS)