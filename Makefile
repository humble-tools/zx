resetdb:
	cat test.cmd | sed -e '/::/d' | redis-cli --pipe

testserver:
	./kill-servers.sh && cargo build && redis-server --dir ./fixtures/ --dbfilename TS.rdb --loadmodule ../target/debug/libzx.dylib

