# Couch DB file compaction

### Note on copying B-trees
See: `couch_btree_copy.erl`

During compaction, CouchDB writes B-trees in a mannner that builds up the leaf
nodes progressively in memory, and writes the nodes to disk after passing a size
threshold, building up a list of pointers to these nodes. After all of the leaf
nodes are written, it then does the same operation recursively on this pointer
list to build the rest of the B-tree.

### Creating the new DB
See: `couch_db_updater:start_copy_compact`

 - A new database file is opened, at the same path as the database being
compacted, with `.compact` added to the file name.
 - If the file already exists
    - and there is a valid header, this should only happen if we are continuing
      compaction because writes have happened to the original DB. See
      "continuing compaction."
    - and there is not a valid header, compaction was interrupted, and we need
      to start over.
 - Any pending writes as of the DB Header snapshot when compaction started are
   written to disk.

### Copying the `by_seq` B-tree
See: `couch_db_updater:initial_copy_compact`

 - The by_seq index in the current DB file is copied. As each value in the
   `by_seq` B-tree is passed over, the corresponding document body is read from
   the old file and written to the new file, and the pointer updated in the
   DocInfo record before being written into the new B-tree.

### Creating the new `by_id` B-tree
See: `couch_db_updater:initial_copy_compact` and `couch_btree_copy:file_sort_output_fun`

We have to now get a list of the DocInfos in Document ID order. Because it's
likely there's a *lot* of them and they'll not comfortably all fit in memory, we
do on disk sorting.

 - A temporary file is created which will be used to do on-disk sorting.
 - The newly written `by_seq` B-Tree in the new file is folded over and each
   DocInfo is written to this temporary file.
 - The file is then sorted with file_sorter, from Erlang's stdlib. This sorts
   files containing Erlang terms. As the first term in a DocInfo record is the
   Document ID, `file_sorter` will do what we want and sort by Document ID.
 - `file_sorter` outputs the sorted values to `couch_btree_copy` to generate the
   new `by_id` B-tree.
 - The temporary file is deleted.

#### Notes/Possible optimizations for new implementation:

 - I think we should be able to write the temporary file at the same time as we are
   writing the new `by_seq` tree, rather than make a second pass over it.
 - Damien has suggested [this on-disk merge sort
   implementation](http://www.efgh.com/software/mergesor.htm). It looks like the
   erlang `file_sorter` niftily allows a callback to be given to send the final
   output to, rather than forcing it to actually be written to disk, which this
   does not seem to allow for. It might be worth looking for something that does
   this so we use less I/O and space, or just modifying this one.

### Finishing up
See: `couch_db_updater:copy_compact` and
`couch_db_updater:handle_call({compact_done, ...`

Once the `by_seq` and `by_id` B-trees have been copied the security object is
copied into the new database, and the security_ptr field of the header updated
in the header, writes are flushed, and the header written to the new file.

At this point the compaction process calls the original DB updater process to
inform it that compaction has finished.

If updates have happened to the original DB since compaction started, the DB
updater process responds with a message telling the compactor to retry
compaction. See "Continuing Compaction" below.

If **no updates** have happened to the original DB since compaction started (the
original DB and the new DB have the same update\_seq), the `local_docs` B-tree is
copied into the new file (local\_doc's bodies are stored inside the B-tree), and
the files are swapped.

#### Swapping files

This operation happens inside the db updater process, and as
such no updates are accepted until it has completed.

 - `couch_file` is told not to close the FD of the old DB until nothing has a
   reference to it, since after the file is unlinked, it will not be possible to
   reopen it.
 - CouchDB switches to the new file's FD for writes and asks `couch_file` to
   prevent any further writes to the old FD. (It may still be open, being used
   for a snapshot read).
 - The compacted file is renamed to the new file path
 - The old file is deleted (unlinked. There may still be open FDs to it).

#### Note on how CouchDB deletes files
See: `couch_file:delete`

To delete files, CouchDB first moves them into a directory used to hold files
intended to be deleted, and adds ".delete" and a random string onto the end of
its filename, then spawns a new erlang process to actually make the
`file:delete` call. This is apparently because deletes may block for a *long*
time.

### Continuing compaction
See: `couch_db_updater:start_copy_compact`

If updates were made to the original DB since compaction began, the compactor
starts over, but will behave differently when finding the compaction
file already exists.

### Catching up compaction
See `EnumBySeqFun` and the `case Retry`'s in `couch_db_updater:copy_compact`

 - The `by_seq` index of the old DB file is scanned from the new db's
   `update_seq` + 1.
 - A list of DocInfos is built up. If the size exceeds a threshold, or scanning
   has finished, the documents are saved into the new file.
 - We again try to finish compaction, from "Finishing up" above.

#### Note on saving docs while continuing compaction
See: `couch_db_updater:copy_docs`

Because some of the updates being copied while catching up compaction may
already exist in the new file, it is necessary to look up the documents being
saved in the new file's `by_id` B-Tree before doing the updates, to check to see
if they already exist and get their sequence numbers for removing from the new
file's `by_seq` index.
