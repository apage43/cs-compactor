#ifndef COUCHSTORE_WRAP_HH
#define COUCHSTORE_WRAP_HH
#include <libcouchstore/couch_db.h>
#include <string>
#include <stdint.h>
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);   \
  void operator=(const TypeName&)
namespace couchstore
{
//## C++ RAII wrappers around Couchstore
class DocumentInfo {
 public:
  DocInfo* get();
  DocInfo* operator->() {
    return get();
  }
  ~DocumentInfo();
 protected:
  friend int do_callback(Db*, DocInfo*, void*);
  DocumentInfo(Db* db, DocInfo* info)
      : docinfo_(info), couchstore_allocated(true) { }
  Db* db_handle_;
  DocInfo* docinfo_;
  bool couchstore_allocated;
 private:
  DISALLOW_COPY_AND_ASSIGN(DocumentInfo);
};

class InfoCallback {
 public:
  virtual int callback(DocumentInfo &info) = 0;
};

class DBHandle {
 public:
  DBHandle(std::string filename, bool create);
  ~DBHandle();
  bool isValid();
  int lastError();
  std::string describeLastError();
  int changes(int seq, InfoCallback& cb);
  Db* get();
  Db* operator->() {
    return get();
  }
  int commit() {
    return commit_all(db_handle_, 0);
  }
 private:
  int last_error_;
  Db* db_handle_;
  DISALLOW_COPY_AND_ASSIGN(DBHandle);
};

class Buffer : public sized_buf {
 public:
  Buffer(size_t sz)
  {
    buf = (char*) malloc(sz);
    size = sz;
  };
  Buffer(char* src, size_t sz)
  {
    buf = (char*) malloc(sz);
    size = sz;
    memcpy(buf, src, sz);
  };
  ~Buffer()
  {
    free(buf);
  };
 private:
  DISALLOW_COPY_AND_ASSIGN(Buffer);
};
}
#endif
