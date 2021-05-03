/*
 *  Copyright (C) 2021 CS416 Rutgers CS
 *	Tiny File System
 *	File:	tfs.c
 *
 */

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>
#include <libgen.h>
#include <limits.h>
#include <stdbool.h>

#include "block.h"
#include "tfs.h"

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#define ROOT_INO 0

char diskfile_path[PATH_MAX];

// Declare your in-memory data structures here
static superblock_t superblock;


/************** Bitmap Functions **************/

int get_avail_ino() {
	/* Read bitmap from disk into stack */
	char block[BLOCK_SIZE];
	bio_read(superblock.i_bitmap_blk, block);
	
	/* Loop through each byte in bitmap*/
	for (int i=0; i < MAX_INUM/8; i++) {
		/* Check if all bits are set to save comparisons */
		if (block[i] == ~0)
			continue;

		/* Loop through by bit */
		for (int j=0; j < 8; j++) {
			int index = i*8+j;
			if (get_bitmap(block, index) == 0) {
				set_bitmap(block, index);
				bio_write(superblock.i_bitmap_blk, block);
				return index;
			}
		}
	}
	return -1;  /* No free bit found */
}

int get_avail_blkno() {
	char block[BLOCK_SIZE];
	bio_read(superblock.d_bitmap_blk, block);
	for (int i=0; i < MAX_DNUM/8; i++) {
		if (block[i] == ~0)
			continue;
		for (int j=0; j < 8; j++) {
			int index = i*8+j;
			if (get_bitmap(block, index) == 0) {
				set_bitmap(block, index);
				bio_write(superblock.d_bitmap_blk, block);
				return superblock.d_start_blk+index;
			}
		}
	}
	return -1;
}

void clear_bmap_ino(int i) {
	char block[BLOCK_SIZE];
	bio_read(superblock.i_bitmap_blk, block);
	unset_bitmap(block, i);
	bio_write(superblock.i_bitmap_blk, block);
}

void clear_bmap_blkno(int i) {
	char block[BLOCK_SIZE];
	bio_read(superblock.d_bitmap_blk, block);
	unset_bitmap(block, i);
	bio_write(superblock.d_bitmap_blk, block);
}

/************** INode Functions **************/

void inode_init(inode_t *inode, uint16_t ino, uint32_t type) {
	inode->ino = ino;
	inode->type = type;
	inode->valid = 1;
	inode->size = 0;
	inode->link = 0;
	for (int i=0; i < 16; i++) {
		inode->direct_ptr[i] = -1;
	}
}

int readi(uint16_t ino, inode_t *inode) {
	int block_num = superblock.i_start_blk + ino / (BLOCK_SIZE/sizeof(inode_t));
	size_t offset = sizeof(inode_t) * (ino % (BLOCK_SIZE/sizeof(inode_t)));

	char block[BLOCK_SIZE];
	bio_read(block_num, block);
	memcpy(inode, block+offset, sizeof(inode_t));

	return 0;
}

int writei(uint16_t ino, inode_t *inode) {
	int block_num = superblock.i_start_blk + ino / (BLOCK_SIZE/sizeof(inode_t));
	size_t offset = sizeof(inode_t) * (ino % (BLOCK_SIZE/sizeof(inode_t)));

	char block[BLOCK_SIZE];
	bio_read(block_num, block);
	memcpy(block+offset, inode, sizeof(inode_t));
	bio_write(block_num, block);

	return 0;
}



/************** Directory Operations **************/

void dirent_init(dirent_t *dirent, uint16_t ino, const char *name, size_t name_len) {
	dirent->valid = 1;
	dirent->ino = ino;
	strncpy(dirent->name, name, name_len);
	dirent->name_len = name_len;
}

// NAME IS NOT NULL TERMINATED
int dir_find(uint16_t ino, const char *fname, size_t name_len, dirent_t *dirent_p) {
	inode_t inode;
	readi(ino, &inode);

	char block[BLOCK_SIZE];
	for (int i=0; i < 16; i++) {
		if (inode.direct_ptr[i] == -1)
			continue;
		
		bio_read(inode.direct_ptr[i], block);
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			dirent_t *dirent = (dirent_t*)block+j;
			if (dirent->valid == 1) {
				if (dirent->name_len == name_len && strncmp(dirent->name, fname, name_len) == 0) {
					memcpy(dirent_p, dirent, sizeof(dirent_t));
					return 0;
				}
			}
		}
	}
	return -1;
}

int dir_add(inode_t *dir_inode, uint16_t f_ino, const char *fname, size_t name_len) {
	/* First make sure dirent doesnt exist in directory */
	dirent_t dirent;
	if (dir_find(dir_inode->ino, fname, name_len, &dirent) == 0)
		return -1;
	
	char block[BLOCK_SIZE];
	for (int i=0; i < 16; i++) {
		/* If dirent block not initialized, allocate new one */
		if (dir_inode->direct_ptr[i] == -1) { 
			int blkno = get_avail_blkno();
			if (blkno == -1)
				return -1;
			dir_inode->direct_ptr[i] = blkno;
			memset(block, 0, BLOCK_SIZE);
		}
		/* Otherwise read dirent block from disk */
		else {
			bio_read(dir_inode->direct_ptr[i], block);
		}

		/* Loop through dirent in block */
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			dirent_t *dirent = (dirent_t*)block+j;
			/* dirent is not valid, free to use */
			if (dirent->valid == 0) {
				dirent_init(dirent, f_ino, fname, name_len);
				bio_write(dir_inode->direct_ptr[i], block);  // persist changes to block
				return 0;
			}
		}
	}
	return -1;
}

int dir_remove(inode_t *dir_inode, const char *fname, size_t name_len) {
	char block[BLOCK_SIZE];
	for (int i=0; i < 16; i++) {
		if (dir_inode->direct_ptr[i] == -1)
			continue;

		bio_read(dir_inode->direct_ptr[i], block);
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			dirent_t *dirent = (dirent_t*)block+j;
			if (dirent->valid == 1) {
				if (dirent->name_len == name_len && strncmp(dirent->name, fname, name_len) == 0) {
					dirent->valid = 0;
					bio_write(dir_inode->direct_ptr[i], block);
					return 0;
				}
			}
		}
	}
	return -1;
}

/* 
 * namei operation
 */
int get_node_by_path(const char *path, uint16_t ino, inode_t *inode) {
	/* Check if path is just root dir "/" */
	if (strcmp(path, "/") == 0) {
		readi(ROOT_INO, inode);
		return 0;
	}

	/* Ignore first char if it is slash */
	if (*path == '/')
		path++;

	/* Everything up to next '/' or '\0' is name of highest level dir/file */
	char *ptr = strchr(path, '/');
	int len = (ptr == NULL) ? strlen(path) : ptr-path;  // length of highest level name

	dirent_t dirent;
	if (dir_find(ino, path, len, &dirent) == 0) {
		if (ptr == NULL) {  // end of path, read into inode and return
			readi(dirent.ino, inode);
			return 0;
		}
		else {
			return get_node_by_path(ptr, dirent.ino, inode);  // recurse
		}
	}
	return -1;  // dir/file not found
}


/************** TFS Fuse Operations **************/

/* 
 * Make file system
 */
int tfs_mkfs() {
	/* Initialize DISKFILE */
	dev_init(diskfile_path);

	/* Initialize superblock struct and info */
	superblock.magic_num = MAGIC_NUM;
	superblock.max_inum = MAX_INUM;
	superblock.max_dnum = MAX_DNUM;
	superblock.i_bitmap_blk = 1;
	superblock.d_bitmap_blk = 2;
	superblock.i_start_blk = superblock.d_bitmap_blk+1;
	int inode_per_blk = BLOCK_SIZE / sizeof(inode_t);
	superblock.d_start_blk = superblock.i_start_blk + (MAX_INUM+(inode_per_blk-1))/inode_per_blk;

	/* Write superblock to disk */
	char block[BLOCK_SIZE];
	memcpy(block, &superblock, sizeof(superblock_t));
	bio_write(0, block);

	/* Write bitmaps to disk */
	memset(block, 0, BLOCK_SIZE);
	bio_write(superblock.i_bitmap_blk, block);
	bio_write(superblock.d_bitmap_blk, block);

	/* Initialize '/' root inode and write to disk */
	inode_t inode;
	inode_init(&inode, get_avail_ino(), 0);
	writei(inode.ino, &inode);
	dir_add(&inode, inode.ino, ".", 1);
	writei(inode.ino, &inode);

	return 0;
}


/* 
 * FUSE file operations
 */
static void *tfs_init(struct fuse_conn_info *conn) {
	if (access(diskfile_path, F_OK) == 0) {
		/* Load DISKFILE and read superblock */
		dev_open(diskfile_path);
		char block[BLOCK_SIZE];
		bio_read(0, block);
		memcpy(&superblock, block, sizeof(superblock_t));
	}
	else {
		/* Initialize DISKFILE, superblock will be initialized in tfs_mkfs() */
		tfs_mkfs();
	}
	return NULL;
}

static void tfs_destroy(void *userdata) {
	/* All in-memory structures are local vars, nothing to free() */
	dev_close();
}

static int tfs_getattr(const char *path, struct stat *stbuf) {
	/* Check dir/file at path exists */
	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == -1)
		return -ENOENT;
	
	/* Fill stbuf with inode info */
	memset(stbuf, 0, sizeof(*stbuf));
	stbuf->st_ino = inode.ino;  // not important
	stbuf->st_mode = (inode.type == 0 ? S_IFDIR : S_IFREG) | 0755;
	stbuf->st_nlink = inode.link;
	stbuf->st_size = inode.size;
	stbuf->st_uid = getuid();
	stbuf->st_gid = getgid();
	time(&stbuf->st_mtime);

	return 0;
}

static int tfs_opendir(const char *path, struct fuse_file_info *fi) {
	inode_t inode;
	/* Check path exists and inode is DIR */
	if (get_node_by_path(path, ROOT_INO, &inode) == 0 && inode.type == 0) {
		return 0;
	}
	else {
		return -1;
	}
}

static int tfs_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
	inode_t inode;
	/* Path doesnt exist or inode is type FILE */
	if (get_node_by_path(path, ROOT_INO, &inode) == -1 || inode.type == 1)
		return -1;

	/* Loop through dirent blocks */
	char block[BLOCK_SIZE];
	for (int i=0; i < 16; i++) {
		if (inode.direct_ptr[i] == -1)
			continue;
	
		bio_read(inode.direct_ptr[i], block);
		for (int j=0; j < BLOCK_SIZE/sizeof(dirent_t); j++) {
			dirent_t *dirent = (dirent_t*)block+j;
			if (dirent->valid == 1) {  // valid dir entry
				filler(buffer, dirent->name, NULL, 0);
			}
		}
	}
	return 0;
}


void parse_name(const char *path, char *parent, char *target) {
	strcpy(parent, path);
	char *p = strrchr(path, '/');
	parent[(p == path) ? 1 : p-path] = 0;
	strcpy(target, p+1);
}

static int tfs_mkdir(const char *path, mode_t mode) {
	char parent[4096], target[208];
	parse_name(path, parent, target);

	inode_t p_inode, t_inode;
	if (get_node_by_path(parent, ROOT_INO, &p_inode) == -1) {
		printf("cant find parent %s", parent);
		return -1;
	}
	
	int ino = get_avail_ino();
	if (ino == -1) {
		return -1;
	}
	if (dir_add(&p_inode, ino, target, strlen(target)) == -1) {
		clear_bmap_ino(ino);
		return -1;
	}
	p_inode.link++;
	writei(p_inode.ino, &p_inode);

	inode_init(&t_inode, ino, 0);
	writei(t_inode.ino, &t_inode);  // THIS LINE IS IMPORTANT, must clear out inode
	dir_add(&t_inode, t_inode.ino, ".", 1);
	dir_add(&t_inode, p_inode.ino, "..", 2);
	writei(t_inode.ino, &t_inode);

	return 0;
}


static int tfs_rmdir(const char *path) {
	char parent[4096], target[208];
	parse_name(path, parent, target);

	inode_t p_inode, t_inode;
	if (get_node_by_path(path, ROOT_INO, &t_inode) == -1)
		return -1;
	get_node_by_path(parent, ROOT_INO, &p_inode);
	
	for (int i=0; i < 16; i++) {
		if (t_inode.direct_ptr[i] != -1) {
			clear_bmap_blkno(t_inode.direct_ptr[i]);
		}
	}
	clear_bmap_ino(t_inode.ino);

	t_inode.valid = 0;
	writei(t_inode.ino, &t_inode);
	dir_remove(&p_inode, target, strlen(target));

	return 0;
}

static int tfs_releasedir(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int tfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
	char parent[4096], target[208];
	parse_name(path, parent, target);

	inode_t p_inode, t_inode;
	if (get_node_by_path(parent, ROOT_INO, &p_inode) == -1)
		return -1;

	int ino = get_avail_ino();
	if (ino == -1)
		return -1;
	inode_init(&t_inode, ino, 1);
	writei(t_inode.ino, &t_inode);

	dir_add(&p_inode, t_inode.ino, target, strlen(target));
	writei(p_inode.ino, &p_inode);

	return 0;
}

static int tfs_unlink(const char *path) {
	char parent[4096], target[208];
	parse_name(path, parent, target);

	inode_t p_inode, t_inode;
	if (get_node_by_path(path, ROOT_INO, &t_inode) == -1)
		return -1;
	get_node_by_path(parent, ROOT_INO, &p_inode);
	
	for (int i=0; i < 16; i++) {
		if (t_inode.direct_ptr[i] != -1) {
			clear_bmap_blkno(t_inode.direct_ptr[i]);
		}
	}
	clear_bmap_ino(t_inode.ino);

	t_inode.valid = 0;
	writei(t_inode.ino, &t_inode);
	dir_remove(&p_inode, target, strlen(target));

	return 0;
}

static int tfs_open(const char *path, struct fuse_file_info *fi) {
	inode_t inode;
	/* Check path exists and inode is FILE */
	if (get_node_by_path(path, ROOT_INO, &inode) == 0 && inode.type == 1) {
		return 0;
	}
	else {
		return -1;
	}
}

static int tfs_read(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {

	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == -1)
		return -1;

	if (size+offset > BLOCK_SIZE * 16) {
		printf("read too large\n");
		return -1;
	}
	
	int start_block = offset / BLOCK_SIZE;
	int start_byte = offset % BLOCK_SIZE;
	int end_block = (offset + size) / BLOCK_SIZE;
	int end_byte = (offset + size) % BLOCK_SIZE;
	
	char block[BLOCK_SIZE];
	bio_read(inode.direct_ptr[start_block], block);

	//one block
	if (size <= BLOCK_SIZE - start_byte) {
		memcpy(buffer, block + start_byte, size);
		return size;
	}
	memcpy(buffer, block + start_byte, BLOCK_SIZE - start_byte);
	buffer +=  BLOCK_SIZE - start_byte;

	//middle blocks
	for (int i=start_block+1; i < end_block; i++) {
		bio_read(inode.direct_ptr[i], block);
		memcpy(buffer, block, BLOCK_SIZE);
		buffer += BLOCK_SIZE;
	}

	// last block
	if (end_byte > 0 && end_block > start_block) {
		bio_read(inode.direct_ptr[end_block], block);
		memcpy(buffer, block, end_byte);
	}
	
	return size;
}

int alloc_d_blk(inode_t *dir_inode, int i) {
	// Allocate a new data block for this directory since it does not exist
	int d_blk_num = get_avail_blkno();
	if (d_blk_num == -1) 
		return -1;

	char block[BLOCK_SIZE];
	memset(block, 0, BLOCK_SIZE);
	bio_write(d_blk_num, block);

	dir_inode->direct_ptr[i] = d_blk_num;
	dir_inode->size += BLOCK_SIZE;
	writei(dir_inode->ino, dir_inode);
	return 0;
}

static int tfs_write(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi) {
	
	inode_t inode;
	if (get_node_by_path(path, ROOT_INO, &inode) == -1) {
		printf("invalid file %s\n", path);
		return -1;
	}

	if (size+offset > BLOCK_SIZE * 16) {
		printf("write too large\n");
		return -1;
	}
	
	int start_block = offset / BLOCK_SIZE;
	int start_byte = offset % BLOCK_SIZE;
	int end_block = (offset + size) / BLOCK_SIZE;
	int end_byte = (offset + size) % BLOCK_SIZE;


	if (inode.direct_ptr[start_block] == -1) {
		if (alloc_d_blk(&inode, start_block) == -1) {
			printf("start block allocation failed\n");
			return -1;
		}
	}
	
	char block[BLOCK_SIZE];
	bio_read(inode.direct_ptr[start_block], block);
	if (size <= BLOCK_SIZE - start_byte) {
		memcpy(block + start_byte, buffer, size);
		bio_write(inode.direct_ptr[start_block], block);
		return size;
	}
	else {
		memcpy(block + start_byte, buffer, BLOCK_SIZE - start_byte);
		bio_write(inode.direct_ptr[start_block], block);
		buffer +=  BLOCK_SIZE - start_byte;
	}
	//middle blocks
	for (int i=start_block+1; i < end_block; i++) {
		if (inode.direct_ptr[i] == -1) {
			if (alloc_d_blk(&inode, i) == -1) {
				printf("%d-th block allocation failed\n", i);
				return -1;
			}
		}
		memcpy(block, buffer, BLOCK_SIZE);
		bio_write(inode.direct_ptr[i], buffer);
		buffer += BLOCK_SIZE;
	}
	// last block
	if (end_byte > 0 && end_block > start_block) {
		if (inode.direct_ptr[end_block] == -1) {
			if (alloc_d_blk(&inode, end_block) == -1) {
				printf("end block allocation failed\n");
				return -1;
			}
		}
		bio_read(inode.direct_ptr[end_block], block);
		memcpy(block, buffer, end_byte);
		bio_write(inode.direct_ptr[end_block], block);
	}
	return size;
}



static int tfs_truncate(const char *path, off_t size) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int tfs_release(const char *path, struct fuse_file_info *fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
	return 0;
}

static int tfs_flush(const char * path, struct fuse_file_info * fi) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}

static int tfs_utimens(const char *path, const struct timespec tv[2]) {
	// For this project, you don't need to fill this function
	// But DO NOT DELETE IT!
    return 0;
}


static struct fuse_operations tfs_ope = {
	.init		= tfs_init,
	.destroy	= tfs_destroy,

	.getattr	= tfs_getattr,
	.readdir	= tfs_readdir,
	.opendir	= tfs_opendir,
	.releasedir	= tfs_releasedir,
	.mkdir		= tfs_mkdir,
	.rmdir		= tfs_rmdir,

	.create		= tfs_create,
	.open		= tfs_open,
	.read 		= tfs_read,
	.write		= tfs_write,
	.unlink		= tfs_unlink,

	.truncate   = tfs_truncate,
	.flush      = tfs_flush,
	.utimens    = tfs_utimens,
	.release	= tfs_release
};


// int main(int argc, char **argv) {
// 	strcpy(diskfile_path, "./DISKFILE");
// 	tfs_mkfs();
// 	tfs_mkdir("/a", 0755);
// 	tfs_rmdir("/a");
// 	tfs_mkdir("/a", 0755);

// 	char path[300];
// 	for (int i=0; i < 100; i++) {
// 		sprintf(path, "/a/dir%d", i);
// 		printf("%d %d\n", i, tfs_mkdir(path, 0755));
// 	}
// 	return 0;
// }

int main(int argc, char **argv) {
	int fuse_stat;
	getcwd(diskfile_path, PATH_MAX);
	strcat(diskfile_path, "/DISKFILE");
	fuse_stat = fuse_main(argc, argv, &tfs_ope, NULL);
	return fuse_stat;
}

