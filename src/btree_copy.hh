#ifndef COUCH_BTREE_COPY_H
#define COUCH_BTREE_COPY_H
#include <libcouchstore/couch_common.h>
#include <utility>
#include <vector>
#include "wrap.hh"
#include <tr1/memory>
#define SHARED_PTR_NS std::tr1
//# B-tree Copy
//Specialized B-Tree writer for copying a B-tree (or at least
//an already sorted list of K/V pairs) into a new B-tree.
namespace couchstore
{
using SHARED_PTR_NS::shared_ptr;
static const uint64_t kChunkThreshold = 1279;
typedef shared_ptr<Buffer> BufPtr;
typedef std::pair<BufPtr, BufPtr> KVPair;
typedef std::vector<KVPair> KVVec;

enum NodeType{
  kKVNode,
  kKPNode
};

#pragma pack(1)
//Ask the compiler not to word-align this struct, as it's used for reading and
//writing to disk.
struct disk_docinfo {
  uint64_t db_seq;
  uint64_t rev_seq;
  uint64_t bp;
  uint32_t len;
  uint32_t id_len;
  uint32_t rev_meta_len;
  uint32_t size;
  uint8_t deleted;
  uint8_t content_meta;
};
#pragma pack()

//## Reduce function interface.
class Reduce {
 public:
  //Reduce called adding a kv pair to a leaf node
  virtual void operator()(KVPair& kv_pair) = 0;
  //Reduce called adding a pointer to a pointer node
  virtual void operator()(Reduce* reduce) = 0;
  //Clone the reduce value (the internal accumulators, but not any temporary
  //buffer used for encoding) so that it can be added to a NodePointer and
  //rereduced over.
  virtual Reduce* clone() = 0;
  //Encode reduce value as a sized_buf (will not need more than one result from
  //this at a time, so it's okay to use a single buffer to return this)
  virtual sized_buf* encode() = 0;
  //Clear the reduce's accumulators (starting a new node)
  virtual void reset() = 0;
  virtual ~Reduce() { };
 protected:
  Reduce() { }
 private:
  DISALLOW_COPY_AND_ASSIGN(Reduce);
};

//## Node Pointer for Copying B-tree
//We use this instead of the regular couchstore node\_pointer so we can keep
//the non-serialized version of the reduce around, so that our Reduces don't
//have to do erlang term parsing.
class NodePointer {
 public:
  NodePointer(uint64_t pointer, Reduce* reduceval, uint64_t subtreesize, BufPtr key) :
      pointer_(pointer), reduce_value_(reduceval),
      subtreesize_(subtreesize), key_(key) {
        encoded_reduce_ = reduce_value_->encode();
      }
  ~NodePointer() {
    delete reduce_value_;
  }
  void encode(char* buf);
  int encodedSize();
  void makeBySeqRoot(DBHandle &db) {
    setAsRoot(&(db.get()->header.by_seq_root));
  }
  void makeByIdRoot(DBHandle &db) {
    setAsRoot(&(db.get()->header.by_id_root));
  }
 protected:
  friend class NodeBuilder;
  uint64_t pointer_;
  sized_buf* encoded_reduce_;
  Reduce* reduce_value_;
  uint64_t subtreesize_;
  BufPtr key_;
 private:
  void setAsRoot(node_pointer**);
  DISALLOW_COPY_AND_ASSIGN(NodePointer);
};

//## B-tree Node Builder
//This class is responsible for the collection of K/V pairs in a B-Tree node
//(both `kv_node`s and `kp_node`s), and writing out the node once the size it
//would be before compression passes `kChunkThreshold`
class NodeBuilder {
 public:
  NodeBuilder(Db* db, Reduce* reduce) : nodesize_(0), db_(db), reduce_(reduce),
    type_(kKVNode), subtreesize_(0) { }
  NodeBuilder(Db* db, Reduce* reduce, NodeType type) : nodesize_(0), db_(db),
    reduce_(reduce), type_(type), subtreesize_(0) { }
  ~NodeBuilder() { }
  int addItem(KVPair kv_pair) {
    (*reduce_)(kv_pair);
    items_.push_back(kv_pair);
    nodesize_ += kv_pair.first->size + kv_pair.second->size + 2;
    if(nodesize_ > kChunkThreshold)
      return flush();
    else
      return 0;
  }
  int addItem(shared_ptr<NodePointer> ptr) {
    (*reduce_)(ptr->reduce_value_);
    pointer_items_.push_back(ptr);
    BufPtr value (new Buffer(ptr->encodedSize()));
    ptr->encode(value->buf);
    items_.push_back(KVPair(ptr->key_, value));
    nodesize_ += ptr->key_->size + value->size + 2;
    subtreesize_ += ptr->subtreesize_;
    if(nodesize_ > kChunkThreshold)
      return flush();
    else
      return 0;
  }
  //`flush` writes the items collected so far as a node and adds a pointer to
  //it to the pointers list.
  int flush();
  //`dumpPointers` moves all the pointers this NodeBuilder has generated to
  //another NodeBuilder's item list (generating pointer nodes)
  int dumpPointers(NodeBuilder& builder) {
    int error = 0;
    for(std::vector<shared_ptr<NodePointer> >::iterator it = pointers_.begin();
        it != pointers_.end(); ++it)
    {
      error = builder.addItem(*it);
      if(error) return error;
    }
    pointers_.clear();
    return error;
  }
  std::vector<shared_ptr<NodePointer> >* pointers() {
    return &pointers_;
  }
  void setType(NodeType type)
  {
    type_ = type;
  }
 protected:
  friend shared_ptr<NodePointer> build_pointers(NodeBuilder&);
  uint64_t nodesize_;
  Db* db_;
  Reduce* reduce_;
  NodeType type_;
  std::vector<KVPair> items_;
  std::vector<shared_ptr<NodePointer> > pointers_;
  std::vector<shared_ptr<NodePointer> > pointer_items_;
  uint64_t subtreesize_;
 private:
  DISALLOW_COPY_AND_ASSIGN(NodeBuilder);
};

shared_ptr<NodePointer> build_pointers(NodeBuilder& builder);
//Callback functions for on-disk merge sort (used to sort our new docinfos by
//ID to create a by ID b-tree.)
//Uses [this on-disk sort implementation](http://www.efgh.com/software/mergesor.htm)
int read_diskdocinfo(FILE* fp, void* buffer, void* ctx);
int write_diskdocinfo(FILE* fp, void* buffer, void* ctx);
int compare_diskdocinfo(void* d1, void* d2, void* ctx);
}
#endif

