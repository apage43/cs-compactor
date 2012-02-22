#ifndef REDUCES_HH
#define REDUCES_HH
#include "btree_copy.hh"
namespace couchstore {
//# Reduce Functions

//## Counting reducer
//Used for the `by_seq` index, is a count of documents below this node in
//the B-tree.
class CountingReduce : public Reduce {
 public:
  CountingReduce();
  void operator()(KVPair& kv_pair);
  void operator()(Reduce* reduce);
  Reduce* clone();
  sized_buf* encode();
  void reset();
  ~CountingReduce();
 protected:
  CountingReduce(uint64_t count);
  uint64_t count_;
  char termbuf_[10];
  sized_buf term_;
};
//## by_id reduce
//Counts live and deleted docs, and adds up their size.
class ByIDReduce : public Reduce {
 public:
  ByIDReduce();
  void operator()(KVPair& kv_pair);
  void operator()(Reduce* reduce);
  void operator()(disk_docinfo* info);
  Reduce* clone();
  sized_buf* encode();
  void reset();
  ~ByIDReduce();
 protected:
  ByIDReduce(uint64_t not_deleted, uint64_t deleted, uint64_t total_size);
  uint64_t not_deleted_count_;
  uint64_t deleted_count_;
  uint64_t total_size_;
  char termbuf_[32];
  sized_buf term_;
};

static const char nil_buf[1] = { 0x6A };
class NullReduce : public Reduce {
 public:
  NullReduce() { term_.buf = const_cast<char*>(nil_buf); term_.size = 1; }
  void operator()(KVPair& kv_pair) { }
  void operator()(Reduce* reduce) { }
  Reduce* clone() { return new NullReduce; }
  sized_buf* encode() { return &term_; }
  void reset() { }
  ~NullReduce() { }
 protected:
  sized_buf term_;
};
}
#endif
