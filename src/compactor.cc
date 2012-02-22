//**Compactor** for couchstore .couch files
#include <fcntl.h>
#include <string>
#include <ei.h>
#include <signal.h>
#include <sys/time.h>
#include <libcouchstore/couch_btree.h>
#include "btree_copy.hh"
#include "wrap.hh"
#include "reduces.hh"
namespace couchstore
{
extern "C"
int merge_sort(FILE *unsorted_file, FILE *sorted_file,
               int (*read)(FILE *, void *, void *),
               int (*write)(FILE *, void *, void *),
               int (*compare)(void *, void *, void *), void *pointer,
               unsigned max_record_size, unsigned long block_size, unsigned long *pcount);

class SeqTreeCopy : public InfoCallback {
 public:
  SeqTreeCopy(NodeBuilder* builder, Db* source, Db* target, int tempfd) :
      builder_(builder), source_(source), target_(target), temp_fd_(tempfd) { }
  int callback(DocumentInfo& info);
 private:
  NodeBuilder* builder_;
  Db* source_;
  Db* target_;
  int temp_fd_;
};

BufPtr number_term(uint64_t num)
{
  int pos = 0;
  BufPtr ret(new Buffer(10));
  ei_encode_ulonglong(ret->buf, &pos, num);
  ret->size = pos;
  return ret;
}

BufPtr binary_term(sized_buf* term)
{
  int pos = 0;
  BufPtr ret(new Buffer(term->size + 5));
  ei_encode_binary(ret->buf, &pos, term->buf, term->size);
  return ret;
}

int copy_seq_index(DBHandle& original_db, DBHandle& new_db, int temp_fd)
{
  int error = 0;
  CountingReduce seq_reduce;
  NodeBuilder output(new_db.get(), &seq_reduce);
  //Run over the `by_seq` B-tree in the original db, copying the document bodies
  //into the new DB and creating a new, balanced `by_seq` btree.
  SeqTreeCopy copier(&output, original_db.get(), new_db.get(), temp_fd);
  error = original_db.changes(0, copier);
  output.flush();
  shared_ptr<NodePointer> seq_root = build_pointers(output);
  seq_root->makeBySeqRoot(new_db);
  return error;
}

BufPtr id_index_value_term(disk_docinfo* info)
{
  char* srcbuf = ((char*) info) + sizeof(disk_docinfo);
  // * 4 bytes - Tuple headers
  // * ID len + 5 - info->rev\_meta encoded as a binary
  // * up to 10 bytes - RevSeq,
  // * rev\_meta len + 5 - info->rev\_meta encoded as a binary
  // * up to 10 bytes - Bp
  // * 2 bytes - Deleted Flag
  // * 2 bytes - ContentMeta
  // * up to 10 bytes - Size
  int pos = 0;
  const size_t maxsize = info->id_len + 5 + info->rev_meta_len + 48;
  BufPtr value (new Buffer(maxsize));
  ei_encode_tuple_header(value->buf, &pos, 6);
  ei_encode_binary(value->buf, &pos, srcbuf, info->id_len);
  srcbuf += info->id_len;
  ei_encode_tuple_header(value->buf, &pos, 2);
  ei_encode_ulonglong(value->buf, &pos, info->rev_seq);
  ei_encode_binary(value->buf, &pos, srcbuf, info->rev_meta_len);
  ei_encode_ulonglong(value->buf, &pos, info->bp);
  ei_encode_ulonglong(value->buf, &pos, info->deleted);
  ei_encode_ulonglong(value->buf, &pos, info->content_meta);
  ei_encode_ulonglong(value->buf, &pos, info->size);
  value->size = pos;
  return value;
}

int build_id_index(DBHandle& new_db, FILE* tempfile)
{
  char tmpbuf[1024];
  disk_docinfo *info = (disk_docinfo*)(tmpbuf);
  ByIDReduce id_reduce;
  NodeBuilder output(new_db.get(), &id_reduce);
  while(read_diskdocinfo(tempfile, tmpbuf, NULL) > 0)
  {
    sized_buf id = {tmpbuf + sizeof(disk_docinfo),
                    info->id_len};
    id_reduce(info);
    output.addItem(KVPair(binary_term(&id),
                          BufPtr(new Buffer((char*) "DICK", 4))));
  }
  output.flush();
  shared_ptr<NodePointer> id_root = build_pointers(output);
  id_root->makeByIdRoot(new_db);
  return 0;
}

int local_doc_copy(couchfile_lookup_request* rq, void* k, sized_buf *v)
{
  sized_buf *key = static_cast<sized_buf*>(k);
  printf("%.*s\n", (int) key->size, key->buf);
  return 0;
}

int copy_local_docs(DBHandle& original_db, DBHandle& new_db)
{
  NullReduce null_reduce;
  NodeBuilder output(new_db.get(), &null_reduce);
  couchfile_lookup_request rq;
  sized_buf tmp;
  rq.cmp.arg = &tmp;
  rq.cmp.compare = ebin_cmp;
  rq.cmp.from_ext = ebin_from_ext;
  sized_buf empty;
  empty.buf = NULL;
  empty.size = 0;
  void* keys[1] = { &empty };
  rq.keys = keys;
  rq.num_keys = 1;
  rq.fold = 1;
  rq.fd = original_db.get()->fd;
  btree_lookup(&rq, original_db.get()->header.local_docs_root->pointer);
  return 0;
}

int finish_compact(DBHandle& original_db, DBHandle& new_db)
{
  int error = 0;
  //Main data and indexes are copied, so now we need to copy the local docs and
  //header info, and write our new header.
  if(original_db->header.local_docs_root)
    error = copy_local_docs(original_db, new_db);
  if(error) return error;
  new_db->header.update_seq = original_db->header.update_seq;
  new_db->header.purge_seq = original_db->header.purge_seq;
  new_db.commit();
  return 0;
}

int compact (std::string& filename)
{
  int error = 0;
  //Open the original database
  DBHandle original_db(filename, false);
  //And create the new file
  DBHandle new_db(filename + ".compact", true);
  //Rewind the file pointer to 0 so that we don't leave a valid header at the
  //beginning of the file.
  new_db->file_pos = 1;
  //We also create a temporary file and store all the docinfos in it, which
  //we will use to build the `by_id` index after we sort it by ID.
  std::string tmpname = filename + ".temp.comact";
  int temp_fd = open(tmpname.c_str(), O_CREAT | O_RDWR, 0744);
  error = copy_seq_index(original_db, new_db, temp_fd);
  close(temp_fd);
  if(error) return error;
  //**TODO** _go make this sorter work on fds and not `FILE*`s_
  FILE* in = fopen(tmpname.c_str(), "r");
  merge_sort(in, in, read_diskdocinfo, write_diskdocinfo,
             compare_diskdocinfo, NULL, 1024, 10000, NULL);
  fseek(in, 0, SEEK_SET);
  error = build_id_index(new_db, in);
  fclose(in);
  finish_compact(original_db, new_db);
  unlink(tmpname.c_str());
  return error;
}

//Encode the erlang term _{(buf), {RevSeq, RevMeta}, Bp, Deleted,
//ContentMeta, Size}_
BufPtr docinfo_term(BufPtr firstterm, DocInfo* info)
{
  // * 4 bytes - Tuple headers
  // * buf->size
  // * up to 10 bytes - RevSeq,
  // * info->rev\_meta.size + 5 - info->rev\_meta encoded as a binary
  // * up to 10 bytes - Bp
  // * 2 bytes - Deleted Flag
  // * 2 bytes - ContentMeta
  // * up to 10 bytes - Size
  int pos = 0;
  const size_t maxsize = firstterm->size + info->rev_meta.size + 48;
  BufPtr value (new Buffer(maxsize));
  ei_encode_tuple_header(value->buf, &pos, 6);
  memcpy(value->buf + pos, firstterm->buf, firstterm->size);
  pos += firstterm->size;
  ei_encode_tuple_header(value->buf, &pos, 2);
  ei_encode_ulonglong(value->buf, &pos, info->rev_seq);
  ei_encode_binary(value->buf, &pos, info->rev_meta.buf, info->rev_meta.size);
  ei_encode_ulonglong(value->buf, &pos, info->bp);
  ei_encode_ulonglong(value->buf, &pos, info->deleted);
  ei_encode_ulonglong(value->buf, &pos, info->content_meta);
  ei_encode_ulonglong(value->buf, &pos, info->size);
  value->size = pos;
  return value;
}

//Called for each item in the source DB's `by_seq` B-tree.
int SeqTreeCopy::callback(DocumentInfo& info)
{
  //Read the document body
  Doc* doc;
  if(open_doc_with_docinfo(source_, info.get(), &doc, 0) < 0)
    return ERROR_READ;
  //Write the document body to the new file.
  off_t new_position = 0;
  if(db_write_buf(target_, &doc->data, &new_position) < 0)
    return ERROR_WRITE;
  free_doc(doc);
  info->bp = new_position;
  //Add the correct KV pair to the new file's by\_seq tree.
  builder_->addItem(KVPair(number_term(info->db_seq),
                           docinfo_term(binary_term(&(info->id)), info.get())));
  //Write the DocInfo value to the temporary file.
  disk_docinfo temp;
  temp.len = sizeof(temp) + info->id.size + info->rev_meta.size;
  temp.id_len = info->id.size;
  temp.db_seq = info->db_seq;
  temp.rev_seq = info->rev_seq;
  temp.rev_meta_len = info->rev_meta.size;
  temp.deleted = info->deleted;
  temp.content_meta = info->content_meta;
  temp.bp = info->bp;
  temp.size = info->size;
  write(temp_fd_, &temp, sizeof(temp));
  write(temp_fd_, info->id.buf, info->id.size);
  write(temp_fd_, info->rev_meta.buf, info->rev_meta.size);
  return 0;
}
}

int main(int argc, char **argv)
{
  if(argc < 2)
    printf("Must specify file to compact.\n");
  std::string filename(argv[1]);
  timeval start, stop;
  gettimeofday(&start, 0);
  int error = couchstore::compact(filename);
  gettimeofday(&stop, 0);
  printf("time: %lu\n", stop.tv_sec - start.tv_sec);
  return error;
}
