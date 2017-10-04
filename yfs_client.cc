// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client()
{
    ec = new extent_client();

}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

#define MAX(x, y) (((x) > (y)) ? (x) : (y))

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;

    r = ec -> get(ino, buf);
    if(r != OK){
      printf("SETATTR: Can not get the content\n");
      return r;
    }

    buf.resize(size);

    r = ec -> put(ino, buf);
    if(r != OK){
      printf("SETATTR: Can not modify the content\n");
      return r;
    }

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    std::string buf;
    
    r = lookup(parent, name, found, ino_out);
    if(r != OK){
      printf("CREATE: Fail to lookup the parent directory\n");
      return r;
    }

    if(found){
      printf("CREATE: File or dir already exists\n");
      return EXIST;
    }

    r = ec -> get(parent, buf);
    if(r != OK){
      printf("CREATE: Can not read the parent directory\n");
      return r;
    }

    r = ec -> create(extent_protocol::T_FILE, ino_out);
    if(r != OK){
      printf("CREATE: Can not create file\n");
      return r;
    }

    buf.append(std::string(name) + "/" + filename(ino_out) + "/");
    r = ec -> put(parent, buf);
    if(r != OK){
      printf("CREATE: Can not modify parent information\n");
    }

    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    std::string buf;
    
    r = lookup(parent, name, found, ino_out);
    if(r != OK){
      printf("MKDIR: Fail to lookup the parent directory\n");
      return r;
    }

    if(found){
      printf("MKDIR: File or dir already exists\n");
      return EXIST;
    }

    r = ec -> get(parent, buf);
    if(r != OK){
      printf("CREATE: Can not read the parent directory\n");
      return r;
    }

    r = ec -> create(extent_protocol::T_DIR, ino_out);
    if(r != OK){
      printf("MKDIR: Can not create dir\n");
      return r;
    }

    buf.append(std::string(name) + "/" + filename(ino_out) + "/");
    r = ec -> put(parent, buf);
    if(r != OK){
      printf("MKDIR: Can not modify parent information\n");
    }

    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::string buf, name_str = std::string(name);
    size_t cur = 0, next = 0;

    r = ec ->get(parent, buf);
    if(r != OK){
      printf("LOOKUP: Parent directory not exists\n");
      return r;
    }

    while(cur < buf.size()){
      next = buf.find("/", cur);
      std::string cur_name = buf.substr(cur, next - cur);
      cur = next + 1;

      next = buf.find("/", cur);
      inum cur_ino = n2i(buf.substr(cur, next - cur));
      cur = next + 1;

      if(cur_name == name_str){
        ino_out = cur_ino;
        found = true;
        return r;
      }
    }

    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;
    size_t cur = 0, next = 0;

    r = ec -> get(dir, buf);
    if(r != OK){
      printf("READDIR: Dir not exists\n");
      return r;
    }


    while(cur < buf.size()){
      struct dirent dirent;
      next = buf.find("/", cur);
      dirent.name = buf.substr(cur, next - cur);
      cur = next + 1;

      next = buf.find("/", cur);
      dirent.inum = n2i(buf.substr(cur, next - cur));
      cur = next + 1;

      list.push_back(dirent);
    }

    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */
    std::string buf;
    
    r = ec -> get(ino, buf);
    if(r != OK){
      printf("READ: Can not get the content\n");
      return r;
    }

    if(off <= (int)buf.size()){
      data = buf.substr(off, size);
    }else{
      data = "";
    }

    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;

    r = ec -> get(ino, buf);
    if(r != OK){
      printf("WRITE: Can not get the content\n");
      return r;
    }

    if(off > (int)buf.size()){
      buf.resize(off, '\0');
      buf.append(data, size);
    }else{
      if(off + size > buf.size()){
        buf.resize(off);
        buf.append(data, size);
      }else{
        buf.replace(off, size, data, size);
      }
    }

    r = ec -> put(ino, buf);
    if(r != OK){
      printf("\n");
      return r;
    }

    bytes_written = size;

    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    std::string buf, entry_name, name_str = std::string(name);
    size_t cur = 0, next = 0, entry_off = 0, entry_len = 0;
    inum ino;
    bool found = false;

    r = ec -> get(parent, buf);
    if(r != OK){
      printf("UNLINK: Can not get the content of parent directory\n");
      return r;
    }

    while(cur < buf.size()){
      entry_off = cur;

      next = buf.find("/", cur);
      entry_name = buf.substr(cur, next - cur);
      cur = next + 1; 

      next = buf.find("/", cur);
      ino = n2i(buf.substr(cur, next - cur));
      cur = next + 1;

      entry_len = next - entry_off;

      if(entry_name == name_str){
        found = true;
        break;
      }
      
    }

    if(!found){
      printf("UNLINK: No such file\n");
      return NOENT;
    }

    buf.erase(entry_off, entry_len);
    r = ec -> put(parent, buf);
    if(r != OK){
      printf("UNLINK: Error while updating parent directory content\n");
      return r;
    }

    r = ec -> remove(ino);
    if(r != OK){
      printf("UNLINK: Can not remove the file\n");
      return r;
    }

    return r;
}

