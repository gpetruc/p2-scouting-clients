CC = c++
CCFLAGS = -W -Wall -Wno-unused-variable -Ofast -march=native   $(shell root-config --cflags)
LIBS = -lstdc++ $(shell root-config --ldflags --libs) -lROOTNTuple
DATAFILE ?= data/Puppi.dump
TKMUDATAFILE ?= data/TkMuons_TM18.dump

ifdef VDT_INCLUDE_DIR
	CCFLAGS += -I$(VDT_INCLUDE_DIR)
endif
ifdef VDT_LIBRARY
	LIBS += -L$(dir $(VDT_LIBRARY))
endif

.PHONY: clean env format run_tests run_test_speed run_test_comp run_test_multi run_test_tkmu 

TARGETS := librootUnpacker.so rootUnpacker.exe
ifneq ($(strip $(TBB)),)
	TARGETS += liveUnpacker.exe
endif
all: $(TARGETS)

BASE_INC := ../unpack.h  ../UnpackerBase.h
BASE_LIB := ../libunpackerBase.so
../libunpackerBase.so: ../UnpackerBase.cc $(BASE_INC)
	cd .. && $(MAKE) libunpackerBase.so

TREE_UNPACKER_SRC := $(wildcard *TTreeUnpacker*.cc)
TREE_UNPACKER_INC := $(TREE_UNPACKER_SRC:.cc=.h)
TREE_UNPACKER_OBJ := $(TREE_UNPACKER_SRC:.cc=.o)
$(TREE_UNPACKER_OBJ): %.o: %.cc %.h TTreeUnpackerBase.h $(BASE_INC)
	$(CC) -fPIC $(CCFLAGS) -c $< -o $@

RNTUPLE_UNPACKER_SRC := $(wildcard *RNTupleUnpacker*.cc)
RNTUPLE_UNPACKER_INC := $(RNTUPLE_UNPACKER_SRC:.cc=.h)
RNTUPLE_UNPACKER_OBJ := $(RNTUPLE_UNPACKER_SRC:.cc=.o)
$(RNTUPLE_UNPACKER_OBJ): %.o: %.cc %.h RNTupleUnpackerBase.h $(BASE_INC) 
	$(CC) -fPIC $(CCFLAGS) -c $< -o $@

RootUnpackMaker.o : RootUnpackMaker.cc RootUnpackMaker.h  $(TREE_UNPACKER_INC) $(RNTUPLE_UNPACKER_INC) $(BASE_INC)
	$(CC) -fPIC $(CCFLAGS) -c $< -o $@

librootUnpacker.so: RootUnpackMaker.o $(RNTUPLE_UNPACKER_OBJ) $(TREE_UNPACKER_OBJ) $(BASE_LIB)
	$(CC) -shared $(CCFLAGS) $(LIBS) $^ -L.. -lunpackerBase -o $@

rootUnpacker.exe: RootUnpackMain.cc librootUnpacker.so
	$(CC) $(CCFLAGS) $(LIBS) $^ -L.. -L. -lunpackerBase -lrootUnpacker -o $@

liveUnpacker.exe: liveUnpacker.cc librootUnpacker.so
	$(CC) $(CCFLAGS) $(LIBS) $^ -L.. -L. -lunpackerBase -lrootUnpacker -I$(TBB)/include -ltbb -o $@

format:
	@clang-format -i *.h *.cc
	@cd rdf_analysis && $(MAKE) format

clean:
	@rm *.exe *.root *.o *.so 2> /dev/null || true

run_tests: rootUnpacker.exe
	@for T in float float24 int raw64; do  \
		./rootUnpacker.exe puppi ttree $$T data/Puppi.dump out.root; \
	done
	@for T in floats coll_float ints coll_int raw64; do  \
		./rootUnpacker.exe puppi rntuple $$T data/Puppi.dump out.root; \
	done
	@for T in float float24 int; do  \
		./rootUnpacker.exe tkmu ttree $$T data/TkMuons_TM18.dump out.root; \
	done
	@for T in floats coll_float; do  \
		./rootUnpacker.exe tkmu rntuple $$T data/TkMuons_TM18.dump out.root; \
	done

run_test_speed: rootUnpacker.exe
	@echo "==== No output ===="
	@./rootUnpacker.exe puppi ttree raw64 $(DATAFILE)
	@./rootUnpacker.exe puppi ttree int $(DATAFILE)
	@./rootUnpacker.exe puppi ttree float $(DATAFILE)
	@echo "==== With output ===="
	@./rootUnpacker.exe puppi ttree raw64 $(DATAFILE) /run/user/$$UID/out.root
	@./rootUnpacker.exe puppi ttree int $(DATAFILE) /run/user/$$UID/out.root
	@./rootUnpacker.exe puppi ttree float $(DATAFILE) /run/user/$$UID/out.root
	@./rootUnpacker.exe puppi ttree float24 $(DATAFILE) /run/user/$$UID/out.root
	@./rootUnpacker.exe puppi rntuple raw64 $(DATAFILE)  /run/user/$$UID/out.root
	@./rootUnpacker.exe puppi rntuple ints $(DATAFILE)  /run/user/$$UID/out.root
	@./rootUnpacker.exe puppi rntuple coll_int $(DATAFILE)  /run/user/$$UID/out.root
	@./rootUnpacker.exe puppi rntuple floats $(DATAFILE)  /run/user/$$UID/out.root
	@./rootUnpacker.exe puppi rntuple coll_float $(DATAFILE)  /run/user/$$UID/out.root 


run_test_multi: rootUnpacker.exe
	@for I in $$(seq 1 6); do cp -v $(DATAFILE) /run/user/$$UID/in$${I}.dump; done
	@./rootUnpacker.exe puppi ttree float /run/user/$$UID/in{1,2,3,4,5,6}.dump;
	@./rootUnpacker.exe puppi ttree float /run/user/$$UID/in{1,2,3,4,5,6}.dump;
	@./rootUnpacker.exe puppi rntuple coll_float /run/user/$$UID/in{1,2,3,4,5,6}.dump
	@./rootUnpacker.exe puppi ttree float  /run/user/$$UID/in{1,2,3,4,5,6}.dump /run/user/$$UID/out.root
	@./rootUnpacker.exe puppi rntuple floats /run/user/$$UID/in{1,2,3,4,5,6}.dump /run/user/$$UID/out.root
	@./rootUnpacker.exe puppi rntuple coll_float /run/user/$$UID/in{1,2,3,4,5,6}.dump /run/user/$$UID/out.root

run_test_comp: rootUnpacker.exe
	@echo "==== With LZ4 output ===="
	@./rootUnpacker.exe puppi ttree int $(DATAFILE) /run/user/$$UID/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi rntuple ints $(DATAFILE)  /run/user/$$UID/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi rntuple coll_int $(DATAFILE)  /run/user/$$UID/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi ttree float $(DATAFILE) /run/user/$$UID/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi ttree float24 $(DATAFILE) /run/user/$$UID/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi rntuple floats $(DATAFILE)  /run/user/$$UID/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi rntuple coll_float $(DATAFILE)  /run/user/$$UID/out.root -j 6 -z lz4,4
	@echo "==== With ZSTD output ===="
	@./rootUnpacker.exe puppi ttree float $(DATAFILE) /run/user/$$UID/out.root -j 6 -z zstd,5
	@./rootUnpacker.exe puppi rntuple coll_float $(DATAFILE)  /run/user/$$UID/out.root -j 6 -z zstd,5

run_test_tkmu: rootUnpacker.exe
	@echo "==== With uncompressed output  ===="
	@./rootUnpacker.exe tkmu ttree int $(TKMUDATAFILE) /run/user/$$UID/out.root
	@./rootUnpacker.exe tkmu ttree float $(TKMUDATAFILE) /run/user/$$UID/out.root
	@./rootUnpacker.exe tkmu rntuple floats $(TKMUDATAFILE) /run/user/$$UID/out.root
	@./rootUnpacker.exe tkmu rntuple coll_float $(TKMUDATAFILE) /run/user/$$UID/out.root
	@echo "==== With LZ4 output ===="
	@./rootUnpacker.exe tkmu ttree int $(TKMUDATAFILE) /run/user/$$UID/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe tkmu ttree float $(TKMUDATAFILE) /run/user/$$UID/out.root -j 6 -z lz4,4