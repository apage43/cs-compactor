#include "wrap.hh"
namespace couchstore
{
DocInfo* DocumentInfo::get()
{
  return docinfo_;
}

DocumentInfo::~DocumentInfo()
{
  if(couchstore_allocated)
    free_docinfo(docinfo_);
}

DBHandle::DBHandle(std::string filename, bool create)
{
  db_handle_ = NULL;
  uint64_t flags = 0;
  if(create)
    flags = COUCH_CREATE_FILES;
  last_error_ = open_db(const_cast<char*>(filename.c_str()),
                                           flags, &db_handle_);
  if(last_error_) db_handle_ = NULL;
}

DBHandle::~DBHandle()
{
  if(db_handle_)
    close_db(db_handle_);
}

bool DBHandle::isValid()
{
  return db_handle_ != NULL;
}

int DBHandle::lastError()
{
  return last_error_;
}

std::string DBHandle::describeLastError()
{
  if(last_error_ == 0)
    return "Success";
  return std::string(describe_error(last_error_));
}

Db* DBHandle::get()
{
  return db_handle_;
}

struct cs_callback_ctx {
  cs_callback_ctx (InfoCallback *callback) : cb(callback) { }
  InfoCallback* cb;
};
int do_callback(Db* db, DocInfo* info, void* ctxptr)
{
  cs_callback_ctx* ctx = static_cast<cs_callback_ctx*>(ctxptr);
  DocumentInfo wrapped(db, info);
  ctx->cb->callback(wrapped);
  return NO_FREE_DOCINFO;
}

int DBHandle::changes(int seq, InfoCallback &cb)
{
  cs_callback_ctx ctx (&cb);
  last_error_ = changes_since(db_handle_, seq, 0, do_callback, static_cast<void*>(&ctx));
  return last_error_;
}
}
