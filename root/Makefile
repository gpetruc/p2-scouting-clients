CC = c++
CCFLAGS = -W -Wall -Wno-unused-variable -Ofast -march=native   $(shell root-config --cflags)
LIBS = -lstdc++ $(shell root-config --ldflags --libs) -lROOTNTuple
DATAFILE ?= data/Puppi.dump
TKMUDATAFILE ?= data/TkMuons_TM18.dump
OUTDIR ?= /run/user/$$UID

ifdef VDT_INCLUDE_DIR
	CCFLAGS += -I$(VDT_INCLUDE_DIR)
endif
ifdef VDT_LIBRARY
	LIBS += -L$(dir $(VDT_LIBRARY))
endif

.PHONY: clean format run_tests run_test_speed run_test_comp run_test_multi run_test_tkmu 

TARGETS := librootUnpacker.so rootUnpacker.exe
ifdef TBB
	TARGETS += liveUnpacker.exe
endif
all: $(TARGETS)

vpath %.so . ..
BASE_INC := ../unpack.h  ../UnpackerBase.h
BASE_LIB := libunpackerBase.so
$(BASE_LIB): ../UnpackerBase.cc $(BASE_INC)
	cd .. && $(MAKE) $@

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

ALLOBJ := RootUnpackMaker.o $(RNTUPLE_UNPACKER_OBJ) $(TREE_UNPACKER_OBJ)

RootUnpackMaker.o : RootUnpackMaker.cc RootUnpackMaker.h  $(TREE_UNPACKER_INC) $(RNTUPLE_UNPACKER_INC) $(BASE_INC)
	$(CC) -fPIC $(CCFLAGS) -c $< -o $@

librootUnpacker.so: $(ALLOBJ) libunpackerBase.so
	$(CC) $(CCFLAGS) $(ALLOBJ) -shared -fPIC $(LIBS) -L..  -lunpackerBase -o $@

rootUnpacker.exe: RootUnpackMain.cc libunpackerBase.so librootUnpacker.so
	$(CC) $(CCFLAGS) $< $(LIBS) -L.. -L. -lunpackerBase -lrootUnpacker -o $@

liveUnpacker.exe: liveUnpacker.cc libunpackerBase.so librootUnpacker.so
	$(CC) $(CCFLAGS) $< $(LIBS) -L.. -L. -lunpackerBase -lrootUnpacker -I$(TBB)/include -ltbb -o $@

format:
	@clang-format -i *.h *.cc
	@cd rdf_analysis && $(MAKE) format

clean:
	@rm *.exe *.root *.o *.so 2> /dev/null || true
	@cd rdf_analysis && $(MAKE) clean

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
	@./rootUnpacker.exe puppi ttree raw64 $(DATAFILE) $(OUTDIR)/out.root
	@./rootUnpacker.exe puppi ttree int $(DATAFILE) $(OUTDIR)/out.root
	@./rootUnpacker.exe puppi ttree float $(DATAFILE) $(OUTDIR)/out.root
	@./rootUnpacker.exe puppi ttree float24 $(DATAFILE) $(OUTDIR)/out.root
	@./rootUnpacker.exe puppi rntuple raw64 $(DATAFILE)  $(OUTDIR)/out.root
	@./rootUnpacker.exe puppi rntuple ints $(DATAFILE)  $(OUTDIR)/out.root
	@./rootUnpacker.exe puppi rntuple coll_int $(DATAFILE)  $(OUTDIR)/out.root
	@./rootUnpacker.exe puppi rntuple floats $(DATAFILE)  $(OUTDIR)/out.root
	@./rootUnpacker.exe puppi rntuple coll_float $(DATAFILE)  $(OUTDIR)/out.root 


run_test_multi: rootUnpacker.exe
	@for I in $$(seq 1 6); do cp -v $(DATAFILE) $(OUTDIR)/in$${I}.dump; done
	@./rootUnpacker.exe puppi ttree float $(OUTDIR)/in{1,2,3,4,5,6}.dump;
	@./rootUnpacker.exe puppi ttree float $(OUTDIR)/in{1,2,3,4,5,6}.dump;
	@./rootUnpacker.exe puppi rntuple coll_float $(OUTDIR)/in{1,2,3,4,5,6}.dump
	@./rootUnpacker.exe puppi ttree float  $(OUTDIR)/in{1,2,3,4,5,6}.dump $(OUTDIR)/out.root
	@./rootUnpacker.exe puppi rntuple floats $(OUTDIR)/in{1,2,3,4,5,6}.dump $(OUTDIR)/out.root
	@./rootUnpacker.exe puppi rntuple coll_float $(OUTDIR)/in{1,2,3,4,5,6}.dump $(OUTDIR)/out.root

run_test_comp: rootUnpacker.exe
	@echo "==== With LZ4 output ===="
	@./rootUnpacker.exe puppi ttree int $(DATAFILE) $(OUTDIR)/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi rntuple ints $(DATAFILE)  $(OUTDIR)/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi rntuple coll_int $(DATAFILE)  $(OUTDIR)/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi ttree float $(DATAFILE) $(OUTDIR)/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi ttree float24 $(DATAFILE) $(OUTDIR)/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi rntuple floats $(DATAFILE)  $(OUTDIR)/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe puppi rntuple coll_float $(DATAFILE)  $(OUTDIR)/out.root -j 6 -z lz4,4
	@echo "==== With ZSTD output ===="
	@./rootUnpacker.exe puppi ttree float $(DATAFILE) $(OUTDIR)/out.root -j 6 -z zstd,5
	@./rootUnpacker.exe puppi rntuple coll_float $(DATAFILE)  $(OUTDIR)/out.root -j 6 -z zstd,5

run_test_tkmu: rootUnpacker.exe
	@echo "==== With uncompressed output  ===="
	@./rootUnpacker.exe tkmu ttree int $(TKMUDATAFILE) $(OUTDIR)/out.root
	@./rootUnpacker.exe tkmu ttree float $(TKMUDATAFILE) $(OUTDIR)/out.root
	@./rootUnpacker.exe tkmu rntuple floats $(TKMUDATAFILE) $(OUTDIR)/out.root
	@./rootUnpacker.exe tkmu rntuple coll_float $(TKMUDATAFILE) $(OUTDIR)/out.root
	@echo "==== With LZ4 output ===="
	@./rootUnpacker.exe tkmu ttree int $(TKMUDATAFILE) $(OUTDIR)/out.root -j 6 -z lz4,4
	@./rootUnpacker.exe tkmu ttree float $(TKMUDATAFILE) $(OUTDIR)/out.root -j 6 -z lz4,4