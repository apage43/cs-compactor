all:
	cd build && make

check: all
	rm build/*.compact
	cp packthis.couch build/packthis.couch
	cd build && ./compactor packthis.couch
	cd build && couch_dbinfo packthis.couch*

doc-clean:
	rm -rf doc/

docco:
	docco src/*.cc src/*.hh
