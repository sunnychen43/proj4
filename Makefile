CC=gcc
CFLAGS=-g -Wall -Wno-unused-value -D_FILE_OFFSET_BITS=64
LDFLAGS=-lfuse

OBJ=tfs.o block.o

%.o: %.c %.h
	$(CC) -c $(CFLAGS) $< -o $@

tfs: $(OBJ)
	$(CC) $(OBJ) $(LDFLAGS) -o tfs

.PHONY: clean
clean:
	rm -f *.o tfs

