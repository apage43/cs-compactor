#include "reduces.hh"
#include "ei.h"
namespace couchstore {
CountingReduce::CountingReduce() : count_(0) { }

CountingReduce::CountingReduce(uint64_t count) : count_(count) { }
CountingReduce::~CountingReduce() { }

//## Counting reducer
//Used on the `by_seq` index.
void CountingReduce::operator()(KVPair& kv_pair)
{
  ++count_;
}

void CountingReduce::operator()(Reduce* reduce)
{
  const CountingReduce* counter = static_cast<CountingReduce*>(reduce);
  count_ += counter->count_;
}

sized_buf* CountingReduce::encode()
{
  int pos = 0;
  ei_encode_ulonglong(termbuf_, &pos, count_);
  term_.buf = termbuf_;
  term_.size = pos;
  return &term_;
}

Reduce* CountingReduce::clone()
{
  return new CountingReduce(count_);
}

void CountingReduce::reset()
{
  count_ = 0;
}
//## by_id reduce
//Counts live and deleted docs, and adds up their size.

ByIDReduce::ByIDReduce() : not_deleted_count_(0),
                           deleted_count_(0), total_size_(0) { }
ByIDReduce::ByIDReduce(uint64_t not_deleted, uint64_t deleted,
                       uint64_t totalsize) : not_deleted_count_(not_deleted),
                       deleted_count_(deleted), total_size_(totalsize) { }

ByIDReduce::~ByIDReduce() { }
void ByIDReduce::operator()(KVPair& kv_pair)
{
  //**_Hackish!_** don't do anything when called from NodeBuilder with a
  //KVPair. Let the function adding the KVPair send us an unencoded
  //disk_docinfo.
}

void ByIDReduce::operator()(disk_docinfo* info)
{
  if(info->deleted)
    ++deleted_count_;
  else
    ++not_deleted_count_;
  total_size_ += info->size;
}

void ByIDReduce::operator()(Reduce* reduce)
{
  const ByIDReduce* reducer = static_cast<ByIDReduce*>(reduce);
  deleted_count_ += reducer->deleted_count_;
  not_deleted_count_ += reducer->not_deleted_count_;
  total_size_ += reducer->total_size_;
}

sized_buf* ByIDReduce::encode()
{
  int pos = 0;
  ei_encode_tuple_header(termbuf_, &pos, 3);
  ei_encode_ulonglong(termbuf_, &pos, not_deleted_count_);
  ei_encode_ulonglong(termbuf_, &pos, deleted_count_);
  ei_encode_ulonglong(termbuf_, &pos, total_size_);
  term_.buf = termbuf_;
  term_.size = pos;
  return &term_;
}

Reduce* ByIDReduce::clone()
{
  return new ByIDReduce(not_deleted_count_, deleted_count_, total_size_);
}

void ByIDReduce::reset()
{
  not_deleted_count_ = 0;
  deleted_count_ = 0;
  total_size_ = 0;
}
}
