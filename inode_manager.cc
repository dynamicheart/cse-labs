#include <ctime>
#include <pthread.h>
#include "inode_manager.h"

#define NORMAL_BLOCK -1
#define BLOCK_AMPLIFICATION 2

// disk layer -----------------------------------------

disk::disk()
{
  pthread_t id;
  int ret;
  bzero(blocks, sizeof(blocks));

  ret = pthread_create(&id, NULL, test_daemon, (void*)blocks);
  if(ret != 0)
	  printf("FILE %s line %d:Create pthread error\n", __FILE__, __LINE__);
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
  pthread_mutex_lock(&bitmap_mutex);
  while(bid < sb.nblocks){
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
          pthread_mutex_unlock(&bitmap_mutex);
          return bid;
        }
      }
    }
  }
  pthread_mutex_unlock(&bitmap_mutex);
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
  char buf[BLOCK_SIZE];

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM / BLOCK_AMPLIFICATION;
  sb.nblocks = BLOCK_NUM / BLOCK_AMPLIFICATION;
  sb.ninodes = INODE_NUM;
  sb.version = 0;
  sb.next_inum = 1;
  sb.seq_end = 1;

  int init_num = 0;
  // root block
  init_num++;
  // supper block
  init_num++;
  // block bitmap
  init_num +=  (sb.nblocks + BPB - 1) / BPB;
  // inode table
  init_num += (sb.ninodes + IPB - 1) / IPB;
  for(int i = 0; i < init_num; i++){
    alloc_block();
  }

  bzero(buf, BLOCK_SIZE);
  memcpy(buf, &sb, sizeof(sb));
  write_block(1, buf);

  pthread_mutex_init(&bitmap_mutex, NULL);
}

inline char parity(char x)
{
  char result = x;
  result = result ^ (x >> 4);
  result = result ^ (x >> 2);
  result = result ^ (x >> 1);
  result &= 0x1;
  return result;
}

// 4 bits to 7 bits
inline char hamming_encode(char raw)
{
  // code'bit structure: [useless][1][2][3][4][5][6][7]
  char code = raw & 0x7;  //Origin data in position 5, 6, 7. Direction from left to right, the highest bit is useless.
  code |= (raw & 0x8) << 1; //Origin data in position 3.
  code |= parity(code & 0x15) << 6; // parity bit in position 1.
  code |= parity(code & 0x13) << 5; // parity bit in position 2.
  code |= parity(code & 0x7) << 3; // parity bit in position 4.
  return code;
}

// 7 bits to 4 bits
inline char hamming_decode(char code)
{
  int position = 0;
  char raw;
  if(((code & 0x40) >> 6) != parity(code & 0x15)) {
    position += 1;
  }
  if(((code & 0x20) >> 5) != parity(code & 0x13)) {
    position += 2;
  }
  if(((code & 0x08) >> 3) != parity(code & 0x7)) {
    position += 4;
  }

  if(position) {
    code ^= 1 << (7 - position);
  }

  raw = code & 0x7;
  raw |= (code & 0x10) >> 1;
  return raw;
}

  void
block_manager::read_block(uint32_t id, char *buf)
{
  char codedata_low[BLOCK_SIZE], codedata_high[BLOCK_SIZE];
  d->read_block(id * BLOCK_AMPLIFICATION, codedata_low);
  d->read_block(id * BLOCK_AMPLIFICATION + 1, codedata_high);
  for(int i = 0; i < BLOCK_SIZE; i++) {
    char low = hamming_decode(codedata_low[i]);
    char high = hamming_decode(codedata_high[i]);

    buf[i] = (high << 4) | low;
  }
  // rectify
  write_block(id, buf);
}

  void
block_manager::write_block(uint32_t id, const char *buf)
{
  char codedata_low[BLOCK_SIZE], codedata_high[BLOCK_SIZE];
  for(int i = 0; i < BLOCK_SIZE; i++) {
    char low = hamming_encode(buf[i] & 0xF);
    char high = hamming_encode((buf[i] >> 4) & 0xF);

    codedata_low[i] = low;
    codedata_high[i] = high;
  }

  d->write_block(id * BLOCK_AMPLIFICATION, codedata_low);
  d->write_block(id * BLOCK_AMPLIFICATION + 1, codedata_high);
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
  pthread_mutex_init(&inode_mutex, NULL);
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
  unsigned int inum;
  unsigned int seq;

  pthread_mutex_lock(&inode_mutex);
  inum = bm->sb.next_inum++;
  seq = bm->sb.seq_end++;
  bzero(buf, BLOCK_SIZE);
  memcpy(buf, &bm->sb, sizeof(bm->sb));
  bm->write_block(1, buf);
  pthread_mutex_unlock(&inode_mutex);

  bm -> read_block(IBLOCK(seq, bm->sb.nblocks), buf);
  inode_t *ino = (inode_t *)buf;

  ino -> type = type;
  ino -> check_point = NORMAL_BLOCK;
  ino -> inum = inum;
  ino -> seq = seq;
  ino -> size = 0;
  ino -> atime = std::time(0);
  ino -> mtime = std::time(0);
  ino -> ctime = std::time(0);

  bm->write_block(IBLOCK(seq, bm->sb.nblocks), buf);

  return inum;
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
  bm -> read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  inode_t *ino = (inode_t *)buf + (inum - 1) % IPB;
  if(ino -> type){
    ino -> type = 0;
    bm -> write_block(IBLOCK(inum, bm->sb.nblocks), buf);
  }
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
  struct inode*
inode_manager::get_inode(uint32_t inum)
{
  inode_t *ino, *ino_disk, *res;
  unsigned int seq;
  char buf[BLOCK_SIZE];
  char buf2[BLOCK_SIZE];
  char buf3[BLOCK_SIZE];
  char new_indirect[BLOCK_SIZE];
  char old_indirect[BLOCK_SIZE];
  bool is_old = false;

  printf("\tim: get_inode %d\n", inum);

  pthread_mutex_lock(&inode_mutex);
  seq = bm -> sb.seq_end - 1;
  while (seq > 0) {
    bm -> read_block(IBLOCK(seq, bm -> sb.nblocks), buf);
    ino = (inode_t *)buf;
    if (ino -> check_point != NORMAL_BLOCK) {
      is_old = true;
    } else if (ino -> inum == inum) {
      if (ino -> type == 0) {
        pthread_mutex_unlock(&inode_mutex);
        return NULL;
      }
      break;
    }
    seq--;
  }

  if (seq == 0) {
    printf("\tim: inum out of range\n");
    pthread_mutex_unlock(&inode_mutex);
    return NULL;
  }

  if (is_old) {
    seq = bm -> sb.seq_end++;
    bzero(buf2, sizeof(buf2));
    memcpy(buf2, &bm->sb, sizeof(bm->sb));
    bm->write_block(1, buf2);

    bm->read_block(IBLOCK(seq, bm->sb.nblocks), buf2);
    ino_disk = (inode_t *)buf2;
    ino_disk->type = ino->type;
    ino_disk->check_point = ino->check_point;
    ino_disk->inum = ino->inum;
    ino_disk->seq = seq;
    ino_disk->size = ino->size;
    ino_disk->atime = ino->atime;
    ino_disk->mtime = ino->mtime;
    ino_disk->ctime = ino->ctime;

    unsigned int block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    for(unsigned int i = 0; i < MIN(block_num, NDIRECT); i++){
      bm->read_block(ino->blocks[i], buf3);
      printf("copying %s    --------  ", buf3);
      ino_disk->blocks[i] = bm->alloc_block();
      bm->write_block(ino_disk->blocks[i], buf3);
    }

    if(block_num > NDIRECT){
      bm->read_block(ino->blocks[NDIRECT], old_indirect);
      for(unsigned int i = NDIRECT; i < block_num; i++){
        bm->read_block(old_indirect[i - NDIRECT], buf3);
        new_indirect[i - NDIRECT] = bm->alloc_block();
        bm->write_block(new_indirect[i - NDIRECT], buf3);
      }
      ino_disk->blocks[NDIRECT] = bm->alloc_block();
      bm->write_block(ino_disk->blocks[NDIRECT], new_indirect);
    }

    bm->write_block(IBLOCK(seq, bm->sb.nblocks), buf2);
    ino = ino_disk;
  }

  res = (inode_t*)malloc(sizeof(inode_t));
  *res = *ino;

  pthread_mutex_unlock(&inode_mutex);
  return res;
}

  void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(ino->seq, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + ino->seq%IPB;
  *ino_disk = *ino;
  printf("\tim: put_inode inode_seq %d\n", ino->seq);
  bm->write_block(IBLOCK(ino->seq, bm->sb.nblocks), buf);
}

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

  unsigned int seq;
  char buf[BLOCK_SIZE];
  inode_t * ino;

  pthread_mutex_lock(&inode_mutex);
  seq = bm->sb.seq_end++;
  bm->read_block(IBLOCK(seq, bm->sb.nblocks), buf);
  ino = (inode_t *)buf;
  ino->check_point = NORMAL_BLOCK;
  ino->type = 0;
  ino->inum = inum;
  bm->write_block(IBLOCK(seq, bm->sb.nblocks), buf);
  bzero(buf, sizeof(buf));
  memcpy(buf, &bm->sb, sizeof(bm->sb));
  bm->write_block(1, buf);
  pthread_mutex_unlock(&inode_mutex);
}

  void
inode_manager::commit()
{
  unsigned int seq;
  char buf[BLOCK_SIZE];
  pthread_mutex_lock(&inode_mutex);
  seq = bm->sb.seq_end++;

  bm->read_block(IBLOCK(seq, bm->sb.nblocks), buf);
  inode_t *ino = (inode_t*) buf;
  ino->check_point = bm->sb.version++;
  bm->write_block(IBLOCK(seq, bm->sb.nblocks), buf);

  bzero(buf, sizeof(buf));
  memcpy(buf, &bm->sb, sizeof(bm->sb));
  bm->write_block(1, buf);
  pthread_mutex_unlock(&inode_mutex);
}

  void
inode_manager::undo()
{
  char buf[BLOCK_SIZE];
  pthread_mutex_lock(&inode_mutex);
  --bm->sb.version;
  while (true) {
      bm->read_block(IBLOCK(--bm->sb.seq_end, bm->sb.nblocks), buf);
      inode_t * ino = (inode_t *)buf;
      if (ino->check_point == (short)bm->sb.version) {
          bzero(buf, sizeof(buf));
          memcpy(buf, &bm->sb, sizeof(bm->sb));
          bm->write_block(1, buf);
          pthread_mutex_unlock(&inode_mutex);
          return;
      }
  }
}

  void
inode_manager::redo()
{
  char buf[BLOCK_SIZE];
  pthread_mutex_lock(&inode_mutex);
  ++bm->sb.version;
  while (true) {
      bm->read_block(IBLOCK(bm->sb.seq_end, bm->sb.nblocks), buf);
      inode_t * ino = (inode_t *)buf;
      if (ino->check_point == (short)bm->sb.version) {
          bzero(buf, sizeof(buf));
          memcpy(buf, &bm->sb, sizeof(bm->sb));
          bm->write_block(1, buf);
          pthread_mutex_unlock(&inode_mutex);
          return;
      }
      ++bm->sb.seq_end;
  }
}
