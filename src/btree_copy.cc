#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include "btree_copy.hh"
#include <ei.h>
#include <libcouchstore/couch_db.h>
#include <list>
#include <utility>
namespace couchstore
{
//## Writing out a node
int NodeBuilder::flush()
{
  if(nodesize_ == 0) return 0;
  //The size of a BTree node is the sum of the size of the KV/KP Pairs, plus
  //the size of a tuple header and 7 byte atom (_kv\_node_ or _kp\_node_), a list header
  //and list tail, and an Erlang term version byte.
  Buffer nodebuf(nodesize_ + 19);
  int bufpos = 0;
  ei_encode_version(nodebuf.buf, &bufpos);
  ei_encode_tuple_header(nodebuf.buf, &bufpos, 2);
  if(type_ == kKVNode)
    ei_encode_atom_len(nodebuf.buf, &bufpos, "kv_node", 7);
  else
    ei_encode_atom_len(nodebuf.buf, &bufpos, "kp_node", 7);
  // ### Encode the item list
  ei_encode_list_header(nodebuf.buf, &bufpos, items_.size());
  for(std::vector<KVPair>::iterator it = items_.begin(); it != items_.end(); ++it)
  {
    //Encode the Key/Value pair as an erlang tuple. The Key and Value are
    //already raw erlang terms, so we just need to write a tuple header, then
    //the two terms.
    ei_encode_tuple_header(nodebuf.buf, &bufpos, 2);
    memcpy(nodebuf.buf + bufpos, (*it).first->buf, (*it).first->size);
    bufpos += (*it).first->size;
    memcpy(nodebuf.buf + bufpos, (*it).second->buf, (*it).second->size);
    bufpos += (*it).second->size;
  }
  ei_encode_empty_list(nodebuf.buf, &bufpos);
  nodebuf.size = bufpos;
  off_t write_position;
  //Write the node to disk, compressed with snappy.
  if(db_write_buf_compressed(db_, &nodebuf, &write_position) < 0)
    return ERROR_WRITE;
  //Create the node pointer
  pointers_.push_back(shared_ptr<NodePointer>
                      (new NodePointer(write_position, reduce_->clone(),
                                       subtreesize_ + nodesize_ + 19,
                                       items_.back().first)));
  subtreesize_ = 0;
  nodesize_ = 0;
  items_.clear();
  pointer_items_.clear();
  reduce_->reset();
  return 0;
}

int NodePointer::encodedSize()
{
  char dummy[10];
  memset(dummy, 0, 10);
  //Size of the reduce value term plus a tuple header.
  int size = 2 + encoded_reduce_->size;
  int pos = 0;
  ei_encode_ulonglong(dummy, &pos, pointer_);
  size += pos;
  pos = 0;
  ei_encode_ulonglong(dummy, &pos, subtreesize_);
  return size + pos;
}

void NodePointer::encode(char* buf)
{
  int pos = 0;
  ei_encode_tuple_header(buf, &pos, 3);
  ei_encode_ulonglong(buf, &pos, pointer_);
  memcpy(buf + pos, encoded_reduce_->buf, encoded_reduce_->size);
  pos += encoded_reduce_->size;
  ei_encode_ulonglong(buf, &pos, subtreesize_);
}

void NodePointer::setAsRoot(node_pointer** root)
{
  sized_buf* reduce = reduce_value_->encode();
  *root = (node_pointer*) malloc(sizeof(node_pointer) + reduce->size);
  (*root)->key.buf = NULL;
  (*root)->key.size = 0;
  (*root)->reduce_value.size = reduce->size;
  (*root)->reduce_value.buf = ((char*) (*root) + sizeof(node_pointer));
  memcpy((*root)->reduce_value.buf, reduce->buf, reduce->size);
  (*root)->pointer = pointer_;
  (*root)->subtreesize = subtreesize_;
}
//## Creating the pointer nodes.
//Once we've written out all of the leaf _kv\_node_s, we create layers of
//pointer nodes in the same manner: collect enough pointers to write out a node,
//then write it out to disk, saving a pointer to it, and repeat until the result
//list of pointers has only a single pointer in it, then save that pointer into
//the DB header.
shared_ptr<NodePointer> build_pointers(NodeBuilder& builder)
{
  NodeBuilder builder_2(builder.db_, builder.reduce_, kKPNode);
  builder.setType(kKPNode);
  shared_ptr<NodePointer> final;
  while(true)
  {
    builder.dumpPointers(builder_2);
    builder_2.flush();
    if(builder_2.pointers()->size() > 1)
    {
      builder_2.dumpPointers(builder);
      builder.flush();
      if(builder.pointers()->size() > 1)
        continue;
      else
      {
        final = builder.pointers()->front();
        break;
      }
    } else
    {
      final = builder_2.pointers()->front();
      break;
    }
  }
  return final;
}

//## Callback functions for on-disk merge sort
//We need to sort the temporary file of DocInfos we created while scanning the
//`by_seq` index by ID, so we can create a `by_id` index.
//
//Using [this on-disk sort implementation](http://www.efgh.com/software/mergesor.htm)
int read_diskdocinfo(FILE* fp, void* buffer, void* ctx)
{
  disk_docinfo* info = (disk_docinfo*) buffer;
  int read = fread(buffer, sizeof(disk_docinfo), 1, fp);
  if(read < 1) return 0;
  int rest = fread((char*) buffer + sizeof(disk_docinfo),
                   info->len - sizeof(disk_docinfo), 1, fp);
  if(rest < 1) return 0;
  return info->len;
}

int write_diskdocinfo(FILE* fp, void* buffer, void* ctx)
{
  disk_docinfo* info = (disk_docinfo*) buffer;
  /* printf("Write info with id %.*s\n", info->id_len, (char*) buffer + sizeof(disk_docinfo)); */
  return fwrite(buffer, info->len, 1, fp);
}

int compare_diskdocinfo(void* d1, void* d2, void* ctx)
{
  disk_docinfo* if1 = (disk_docinfo*) d1;
  disk_docinfo* if2 = (disk_docinfo*) d2;
  char *id1 = (char*) d1 + sizeof(disk_docinfo);
  char *id2 = (char*) d2 + sizeof(disk_docinfo);
  unsigned int size;
  if(if2->id_len < if1->id_len)
  {
    size = if2->id_len;
  }
  else
  {
    size = if1->id_len;
  }
  int cmp = memcmp(id1, id2, size);
  if(cmp == 0)
  {
    if(size < if2->id_len)
      return -1;
    else if(size < if1->id_len)
      return 1;
  }
  return cmp;
}
}
