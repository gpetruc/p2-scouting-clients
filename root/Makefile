CC = c++
CCFLAGS = -W -Wall -Wno-unused-variable -Ofast -march=native   $(shell root-config --cflags)
LIBS = -lstdc++ $(shell root-config --ldflags --libs) -lROOTNTuple
DATAFILE ?= data/Puppi.dump
TKMUDATAFILE ?= data/TkMuons_TM18.dump
LCG ?= /cvmfs/sft.cern.ch/lcg/views/dev3cuda/latest/x86_64-centos8-gcc11-opt

.PHONY: clean env format run_tests run_test_speed run_test_comp run_test_multi run_test_tkmu run_test_old

all: unpack.exe rntuple_unpack.exe treeUnpacker.exe rntupleUnpacker.exe

MyDict.cxx: puppi.h Linkdef.h
	rootcling -f $@ $^

unpack.exe : unpack.cc ../unpack.h ../UnpackerBase.h
	$(CC) $(CCFLAGS) $(LIBS) $< -o $@

rntuple_unpack.exe: rntuple_unpack.cc ../unpack.h MyDict.cxx
	$(CC) $(CCFLAGS) $(LIBS) $< MyDict.cxx -o $@

TREE_UNPACKER_SRC := $(wildcard *TTreeUnpacker*.cc)
TREE_UNPACKER_OBJ := $(TREE_UNPACKER_SRC:.cc=.o)
$(TREE_UNPACKER_OBJ): %.o: %.cc %.h ../unpack.h TTreeUnpackerBase.h ../UnpackerBase.h 
	$(CC) $(CCFLAGS) -c $< -o $@

treeUnpacker.exe: TTreeUnpackMain.cc $(TREE_UNPACKER_OBJ)
	$(CC) $(CCFLAGS) $(LIBS) $^ -o $@

RNTUPLE_UNPACKER_SRC := $(wildcard *RNTupleUnpacker*.cc)
RNTUPLE_UNPACKER_OBJ := $(RNTUPLE_UNPACKER_SRC:.cc=.o)
$(RNTUPLE_UNPACKER_OBJ): %.o: %.cc %.h ../unpack.h RNTupleUnpackerBase.h ../UnpackerBase.h 
	$(CC) $(CCFLAGS) -c $< -o $@

rntupleUnpacker.exe: RNTupleUnpackMain.cc $(RNTUPLE_UNPACKER_OBJ)
	$(CC) $(CCFLAGS) $(LIBS) $^ -o $@

liveUnpacker.exe: liveUnpacker.cc $(RNTUPLE_UNPACKER_OBJ) $(TREE_UNPACKER_OBJ)
	$(CC) $(CCFLAGS) $(LIBS) $^ -I$(LCG)/include -ltbb  -o $@

env:
	@echo "source /cvmfs/sft.cern.ch/lcg/views/dev3cuda/latest/x86_64-centos8-gcc11-opt/setup.sh"

format:
	@clang-format -i puppi.h unpack.cc rntuple_unpack.cc
	@clang-format -i $(TREE_UNPACKER_SRC) $(TREE_UNPACKER_SRC:.cc=.h) TTreeUnpackMain.cc 
	@clang-format -i $(RNTUPLE_UNPACKER_SRC) $(RNTUPLE_UNPACKER_SRC:.cc=.h) RNTupleUnpackMain.cc 
	@clang-format -i liveUnpacker.cc 

clean:
	@rm *.exe *.root *.o  MyDict.cxx MyDict_rdict.pcm 2> /dev/null || true

run_tests: treeUnpacker.exe rntupleUnpacker.exe
	@for T in float float24 int raw64; do  \
		./treeUnpacker.exe puppi $$T data/Puppi.dump out.root; \
	done
	@for T in floats coll_float ints coll_int raw64; do  \
		./rntupleUnpacker.exe puppi $$T data/Puppi.dump out.root; \
	done
	@for T in float float24 int; do  \
		./treeUnpacker.exe tkmu $$T data/TkMuons_TM18.dump out.root; \
	done
	@for T in floats coll_float; do  \
		./rntupleUnpacker.exe tkmu $$T data/TkMuons_TM18.dump out.root; \
	done

run_test_speed: treeUnpacker.exe rntupleUnpacker.exe
	@echo "==== No output ===="
	@./treeUnpacker.exe puppi raw64 $(DATAFILE)
	@./treeUnpacker.exe puppi int $(DATAFILE)
	@./treeUnpacker.exe puppi float $(DATAFILE)
	@echo "==== With output ===="
	@./treeUnpacker.exe puppi raw64 $(DATAFILE) /run/user/$$UID/out.root
	@./treeUnpacker.exe puppi int $(DATAFILE) /run/user/$$UID/out.root
	@./treeUnpacker.exe puppi float $(DATAFILE) /run/user/$$UID/out.root
	@./treeUnpacker.exe puppi float24 $(DATAFILE) /run/user/$$UID/out.root
	@./rntupleUnpacker.exe puppi raw64 $(DATAFILE)  /run/user/$$UID/out.root
	@./rntupleUnpacker.exe puppi ints $(DATAFILE)  /run/user/$$UID/out.root
	@./rntupleUnpacker.exe puppi coll_int $(DATAFILE)  /run/user/$$UID/out.root
	@./rntupleUnpacker.exe puppi floats $(DATAFILE)  /run/user/$$UID/out.root
	@./rntupleUnpacker.exe puppi coll_float $(DATAFILE)  /run/user/$$UID/out.root


run_test_multi: treeUnpacker.exe rntupleUnpacker.exe
	@for I in $$(seq 1 6); do cp -v $(DATAFILE) /run/user/$$UID/in$${I}.dump; done
	@./treeUnpacker.exe puppi float /run/user/$$UID/in{1,2,3,4,5,6}.dump;
	@./treeUnpacker.exe puppi float /run/user/$$UID/in{1,2,3,4,5,6}.dump;
	@./rntupleUnpacker.exe puppi coll_float /run/user/$$UID/in{1,2,3,4,5,6}.dump
	@./treeUnpacker.exe puppi float  /run/user/$$UID/in{1,2,3,4,5,6}.dump /run/user/$$UID/out.root
	@./rntupleUnpacker.exe puppi floats /run/user/$$UID/in{1,2,3,4,5,6}.dump /run/user/$$UID/out.root
	@./rntupleUnpacker.exe puppi coll_float /run/user/$$UID/in{1,2,3,4,5,6}.dump /run/user/$$UID/out.root

run_test_comp: treeUnpacker.exe puppi rntupleUnpacker.exe
	@echo "==== With LZ4 output ===="
	@./treeUnpacker.exe puppi int $(DATAFILE) /run/user/$$UID/out.root -j 6 -z lz4,4
	@./rntupleUnpacker.exe puppi ints $(DATAFILE)  /run/user/$$UID/out.root -j 6 -z lz4,4
	@./rntupleUnpacker.exe puppi coll_int $(DATAFILE)  /run/user/$$UID/out.root -j 6 -z lz4,4
	@./treeUnpacker.exe puppi float $(DATAFILE) /run/user/$$UID/out.root -j 6 -z lz4,4
	@./treeUnpacker.exe puppi float24 $(DATAFILE) /run/user/$$UID/out.root -j 6 -z lz4,4
	@./rntupleUnpacker.exe puppi floats $(DATAFILE)  /run/user/$$UID/out.root -j 6 -z lz4,4
	@./rntupleUnpacker.exe puppi coll_float $(DATAFILE)  /run/user/$$UID/out.root -j 6 -z lz4,4
	@echo "==== With ZSTD output ===="
	@./treeUnpacker.exe puppi float $(DATAFILE) /run/user/$$UID/out.root -j 6 -z zstd,5
	@./rntupleUnpacker.exe puppi coll_float $(DATAFILE)  /run/user/$$UID/out.root -j 6 -z zstd,5

run_test_tkmu: treeUnpacker.exe rntupleUnpacker.exe
	@echo "==== With uncompressed output  ===="
	@./treeUnpacker.exe tkmu int $(TKMUDATAFILE) /run/user/$$UID/out.root
	@./treeUnpacker.exe tkmu float $(TKMUDATAFILE) /run/user/$$UID/out.root
	@./rntupleUnpacker.exe tkmu floats $(TKMUDATAFILE) /run/user/$$UID/out.root
	@./rntupleUnpacker.exe tkmu coll_float $(TKMUDATAFILE) /run/user/$$UID/out.root
	@echo "==== With LZ4 output ===="
	@./treeUnpacker.exe tkmu int $(TKMUDATAFILE) /run/user/$$UID/out.root -j 6 -z lz4,4
	@./treeUnpacker.exe tkmu float $(TKMUDATAFILE) /run/user/$$UID/out.root -j 6 -z lz4,4

run_test_old: unpack.exe rntuple_unpack.exe 
	@echo "==== No output ===="
	@./unpack.exe combined raw64 $(DATAFILE)
	@./unpack.exe separate int $(DATAFILE)
	@./unpack.exe combined int $(DATAFILE)
	@./unpack.exe separate float $(DATAFILE)
	@./unpack.exe combined float $(DATAFILE)
	@./unpack.exe combined float24 $(DATAFILE)
	@echo "==== With output ===="
	@./unpack.exe combined raw64 $(DATAFILE)  /run/user/$$UID/out.root
	@./unpack.exe separate int $(DATAFILE) /run/user/$$UID/out.root
	@./unpack.exe combined int $(DATAFILE) /run/user/$$UID/out.root
	@./unpack.exe separate float $(DATAFILE) /run/user/$$UID/out.root
	@./unpack.exe combined float $(DATAFILE) /run/user/$$UID/out.root
	@./unpack.exe combined float24 $(DATAFILE) /run/user/$$UID/out.root
	@./rntuple_unpack.exe combined raw64 $(DATAFILE) /run/user/$$UID/out.root
	@./rntuple_unpack.exe combined float $(DATAFILE) /run/user/$$UID/out.root
	@./rntuple_unpack.exe combined_coll float $(DATAFILE) /run/user/$$UID/out.root
	@./rntuple_unpack.exe combined_struct float $(DATAFILE) /run/user/$$UID/out.root
	@echo "==== With LZ4 output ===="
	@./unpack.exe -j 6 separate int $(DATAFILE) /run/user/$$UID/out.root -z lz4,4
	@./unpack.exe -j 6 combined int $(DATAFILE) /run/user/$$UID/out.root -z lz4,4
	@./unpack.exe -j 6 separate float $(DATAFILE) /run/user/$$UID/out.root -z lz4,4
	@./unpack.exe -j 6 combined float $(DATAFILE) /run/user/$$UID/out.root -z lz4,4
	@./unpack.exe -j 6 combined float24 $(DATAFILE) /run/user/$$UID/out.root -z lz4,4
	@./rntuple_unpack.exe -j 6 combined float $(DATAFILE)  /run/user/$$UID/out.root -z lz4,4
	@echo "==== With ZSTD output ===="
	@./unpack.exe -j 6 combined int $(DATAFILE) /run/user/$$UID/out.root -z zstd,5
	@./unpack.exe -j 6 combined float $(DATAFILE) /run/user/$$UID/out.root -z zstd,5
	@./unpack.exe -j 6 combined float24 $(DATAFILE) /run/user/$$UID/out.root -z zstd,5
	@./rntuple_unpack.exe -j 6 combined float $(DATAFILE)  /run/user/$$UID/out.root -z zstd,5


