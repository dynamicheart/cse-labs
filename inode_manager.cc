#include <ctime>
#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

  void
disk::read_block(blockid_t id, char *buf)
{
  /*
   *your lab1 code goes here.
   *if id is smaller than 0 or larger than BLOCK_NUM 
   *or buf is null, just return.
   *put the content of target block into buf.
   *hint: use memcpy
   */
  if(id < 0 || id >= BLOCK_NUM || buf == NULL){
    printf("\tim: invalid block id\n");
    return;
  }
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

  void
disk::write_block(blockid_t id, const char *buf)
{
  /*
   *your lab1 code goes here.
   *hint: just like read_block
   */
  if(id < 0 || id >= BLOCK_NUM || buf == NULL){
    printf("\tim: invalid block id\n");
    return;
  }
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
  blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.

   *hint: use macro IBLOCK and BBLOCK.
   use bit operation.
   remind yourself of the layout of disk.
   */
  char block[BLOCK_SIZE];
  char current_byte;
  char mask;
  blockid_t bid = 0;
  while(bid < BLOCK_NUM){
    read_block(BBLOCK(bid), block);
    for(int i = 0; i < BLOCK_SIZE; i ++){
      current_byte = block[i];
      for(int j = 0; j < 8; j++){
        mask = 0x1 << j;
        if(current_byte & mask){
          bid++;
        }else{
          block[i] = current_byte | mask;
          write_block(BBLOCK(bid), block);
          return bid;
        }
      }
    }
  }
  printf("\tim: out of blocks\n");
  exit(0);
}

  void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  int idx = (id % BPB) / 8;
  int offset = (id % BPB) % 8;
  char mask = 0x1 << offset;
  char buf[BLOCK_SIZE];

  read_block(BBLOCK(id), buf);
  buf[idx] = buf[idx] & mask;
  write_block(id, buf);
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  int init_num = 0;
  // root block
  init_num++;
  // supper block
  init_num++;
  // block bitmap
  init_num +=  (BLOCK_NUM + BPB - 1) / BPB;
  // inode table
  init_num += (INODE_NUM + IPB - 1) / IPB;
  for(int i = 0; i < init_num; i++){
    alloc_block();
  }
}

  void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

  void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
  uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().

   * if you get some heap memory, do not forget to free it.
   */
  char buf[BLOCK_SIZE];
  uint32_t inum = 1;

  while(inum <= INODE_NUM){
    bm -> read_block(IBLOCK(inum, BLOCK_NUM), buf);
    for(int i = 0; i < IPB && inum <= INODE_NUM; i++){
      inode_t *inode = (inode_t *)buf + i;
      if(inode-> type == 0){
        inode -> type = type;
        inode -> size = 0;

        inode -> atime = std::time(0);
        inode -> mtime = std::time(0);
        inode -> ctime = std::time(0);

        bm->write_block(IBLOCK(inum, BLOCK_NUM), buf);
        return inum;
      }else{
        inum++;
      }
    }
  }
  exit(1);
}

  void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   * do not forget to free memory if necessary.
   */
  char buf[BLOCK_SIZE];
  bm -> read_block(IBLOCK(inum, BLOCK_NUM), buf);
  inode_t *ino = (inode_t *)buf + (inum - 1) % IPB; 
  if(ino -> type){
    ino -> type = 0;
    bm -> write_block(IBLOCK(inum, BLOCK_NUM), buf);
  }
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
  struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

  void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
  void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  inode_t *ino = get_inode(inum);
  char block[BLOCK_SIZE];
  char indirect_blocks[BLOCK_SIZE];
  char *buf = (char *) malloc(ino -> size);
  int block_num = (ino -> size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  int offset = 0;
  
  
  if(block_num <= NDIRECT){
    for(int i = 0; i < block_num; i++){
      if(ino -> size - offset > BLOCK_SIZE){
        bm -> read_block(ino -> blocks[i], buf + offset);
        offset += BLOCK_SIZE;
      }else {
        bm -> read_block(ino -> blocks[i], block);
        memcpy(buf + offset, block, ino -> size - offset);
        offset += ino -> size - offset;
      }
    }
  }else{
    for(int i = 0; i < NDIRECT; i++){
      bm -> read_block(ino -> blocks[i], block);
      bm -> read_block(ino -> blocks[i], buf + offset);
      offset += BLOCK_SIZE;
    }

    bm -> read_block(ino -> blocks[NDIRECT], indirect_blocks);
    for(int i = 0; i < block_num - NDIRECT; i++){
      if(ino -> size - offset >= BLOCK_SIZE){
        bm -> read_block(((blockid_t *) indirect_blocks)[i], block);
        bm -> read_block(((blockid_t *) indirect_blocks)[i], buf + offset);
        offset += BLOCK_SIZE;
      }else {
        bm -> read_block(((blockid_t *) indirect_blocks)[i], block);
        memcpy(buf + offset, block, ino -> size - offset);
        offset += ino -> size - offset;
      }
    }
  }

  ino -> atime = std::time(0);
  ino -> ctime = std::time(0);
  *size = ino -> size;
  *buf_out = buf;

  put_inode(inum, ino);
  free(ino);
}

/* alloc/free blocks if needed */
  void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode.
   * you should free some blocks if necessary.
   */
  inode_t *ino = get_inode(inum);
  char block[BLOCK_SIZE];
  char indirect_blocks[BLOCK_SIZE];
  int old_block_num = (ino -> size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  int new_block_num = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  int offset = 0;

  bm -> read_block(62, block);
  // allocate new blocks or free some blocks
  if(new_block_num > old_block_num){
    if(old_block_num > NDIRECT){
      bm -> read_block(ino -> blocks[NDIRECT], indirect_blocks);
      for(int i = 0; i < new_block_num - old_block_num; i++){
        int bid = bm -> alloc_block();
        ((blockid_t *)indirect_blocks)[old_block_num - NDIRECT + i] = bid;
      }
      bm -> write_block(ino -> blocks[NDIRECT], indirect_blocks);
    }else if(old_block_num <= NDIRECT && new_block_num > NDIRECT){
      ino -> blocks[NDIRECT] = bm -> alloc_block();
      bzero(indirect_blocks, BLOCK_SIZE);
      bm -> read_block(ino -> blocks[NDIRECT], indirect_blocks);
      for(int i = 0; i < NDIRECT - old_block_num; i++){
        ino -> blocks[old_block_num + i] = bm -> alloc_block();
      }
      for(int i = 0; i < new_block_num - NDIRECT; i++){
        ((blockid_t *)indirect_blocks)[i] = bm -> alloc_block();
      }
      bm -> write_block(ino -> blocks[NDIRECT], indirect_blocks);
    }else{
      for(int i = 0; i < new_block_num - old_block_num; i++){
        ino -> blocks[old_block_num + i] = bm -> alloc_block();
      }
    }
  }else if(new_block_num < old_block_num){
    if(old_block_num <= NDIRECT){
      for(int i = 0; i < old_block_num - new_block_num; i++){
        bm -> free_block(ino -> blocks[new_block_num + i]);
        ino -> blocks[new_block_num + i] = 0;
      }
    }else if(old_block_num > NDIRECT && new_block_num <= NDIRECT){
      bm -> read_block(ino -> blocks[NDIRECT], indirect_blocks);
      for(int i = 0; i < old_block_num - NDIRECT; i++){
        bm -> free_block(((blockid_t *)indirect_blocks)[i]);
        ((blockid_t *)indirect_blocks)[i]= 0;
      }
      bm -> write_block(ino -> blocks[NDIRECT], indirect_blocks);
      bm -> free_block(ino -> blocks[NDIRECT]);
      ino -> blocks[NDIRECT] = 0;
    }else{
      bm -> read_block(ino -> blocks[NDIRECT], indirect_blocks);
      for(int i = 0; i < old_block_num - new_block_num; i++){
        bm -> free_block(((blockid_t *)indirect_blocks)[new_block_num - NDIRECT + i]);
        ((blockid_t *)indirect_blocks)[new_block_num - NDIRECT + i] = 0;
      }
      bm -> write_block(ino -> blocks[NDIRECT], indirect_blocks);
    }
  }


  bm -> read_block(62, block);
  if(new_block_num <= NDIRECT){
    for(int i = 0; i < new_block_num; i++){
      if(size - offset > BLOCK_SIZE){
        bm -> write_block(ino -> blocks [i], buf + offset);
        offset += BLOCK_SIZE;
      }else{
        memcpy(block, buf + offset, size - offset);
        bm -> write_block(ino -> blocks[i], block);
        offset += size - offset;
      }
    }
  }else{
    
    bm -> read_block(62, block);
    for(int i = 0; i < NDIRECT; i++){
      bm -> write_block(ino -> blocks[i], buf + offset); 
      offset += BLOCK_SIZE;
    }

    bm -> read_block(ino -> blocks[NDIRECT], indirect_blocks);
    for(int i = 0; i < new_block_num - NDIRECT; i++){
      if(size - offset > BLOCK_SIZE){
        bm -> write_block(((blockid_t *) indirect_blocks)[i], buf + offset); 
        bm -> read_block(((blockid_t *) indirect_blocks)[i], block);
        offset += BLOCK_SIZE;
      }else{
        memcpy(block, buf + offset, size - offset);
        bm -> write_block(((blockid_t *) indirect_blocks)[i], block);
        offset += size - offset;
      }
    }
  }

  ino -> mtime = std::time(0);
  ino -> ctime = std::time(0);
  ino -> size = size;
  put_inode(inum, ino);
  free(ino);
}

  void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t *ino = get_inode(inum);
  if(ino){
    a.type = ino -> type;
    a.atime = ino -> atime;
    a.mtime = ino -> mtime;
    a.ctime = ino -> ctime;
    a.size = ino -> size;

    free(ino);
  }
}

  void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   * do not forget to free memory if necessary.
   */
  inode_t *ino = get_inode(inum);
  char indirect_blocks[BLOCK_SIZE];

  for(int i = 0; i < NDIRECT; i++){
    if(ino -> blocks[i]){
      bm -> free_block(ino -> blocks[i]);
      ino -> blocks[i] = 0;
    }else{
      break;
    }
  }
    
  if(ino -> blocks[NDIRECT]){
    bm -> read_block(ino -> blocks[NDIRECT], indirect_blocks);
    for(unsigned int i = 0; i < NINDIRECT; i++){
      if((blockid_t *) indirect_blocks + i){
        bm -> free_block(((blockid_t *) indirect_blocks)[i]);
        ((blockid_t *) indirect_blocks)[i] = 0;
      }else{
        break;
      }
    }
    bm -> write_block(ino -> blocks[NDIRECT], indirect_blocks);
    ino -> blocks[NDIRECT] = 0;
  }
    
  free_inode(inum);
  free(ino);
}
