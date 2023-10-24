CC = c++
CCFLAGS = -ggdb --std=c++17 -W -Wall -Wno-unused-variable -Ofast -march=native   $(shell root-config --cflags)
LIBS = -lstdc++ $(shell root-config --ldflags --libs) -lROOTNTuple  
EXTRA_HEADERS = 
EXTRA_OBJS :=  
ifdef VDT_INCLUDE_DIR
	CCFLAGS += -I$(VDT_INCLUDE_DIR)
endif
ifdef VDT_LIBRARY
	LIBS += -L$(dir $(VDT_LIBRARY))
endif

ifdef ARROW_INCLUDE
	CCFLAGS += -I$(ARROW_INCLUDE) -I. -DUSE_ARROW=1
	LIBS += -L$(ARROW_LIB) -larrow
	EXTRA_HEADERS += RArrowDS2.hxx
	EXTRA_OBJS += RArrowDS2.o 
endif

.PHONY: clean format 

TARGETS := run.exe
ifdef TBB
	TARGETS += liveAnalysis.exe
endif
all: $(TARGETS)

ANALYSES_SRC := analysis.cc w3piExample2022.cc
ANALYSES_OBJ := $(ANALYSES_SRC:.cc=.o)
ANALYSES_H := $(ANALYSES_SRC:.cc=.h)

ifdef ARROW_INCLUDE
RArrowDS2.o : RArrowDS2.cxx RArrowDS2.hxx
	$(CC) $(CCFLAGS) -c $< -o $@
endif

$(ANALYSES_OBJ): %.o: %.cc %.h analysis.h $(EXTRA_HEADERS)
	$(CC) $(CCFLAGS) -c $< -o $@

run.exe: run.cc $(ANALYSES_OBJ) $(EXTRA_OBJS)
	$(CC) $(CCFLAGS) $^ $(LIBS) -o $@

liveAnalysis.exe: liveAnalysis.cc $(ANALYSES_OBJ) $(EXTRA_OBJS)
	$(CC) $(CCFLAGS) $^ $(LIBS) -I$(LCG)/include -ltbb -o $@

format:
	@clang-format -i *.cc *.h *.cxx *.hxx

clean:
	@rm *.o *.exe *.root *.raw *.arrow  2> /dev/null || true

testInput_TTree.root:
	../rootUnpacker.exe puppi ttree float ../data/Puppi.dump  testInput_TTree.root

testInput_TTree_int.root:
	../rootUnpacker.exe puppi ttree int ../data/Puppi.dump  testInput_TTree_int.root

testInput_RNTuple_coll.root:
	../rootUnpacker.exe puppi rntuple coll_float ../data/Puppi.dump  testInput_RNTuple_coll.root

testInput_RNTuple_vec.root:
	../rootUnpacker.exe puppi rntuple floats ../data/Puppi.dump  testInput_RNTuple_vec.root

testInput.arrow:
ifdef ARROW_INCLUDE
	../../apache/apacheUnpacker.exe puppi ipcstream float ../data/Puppi.dump testInput.arrow
else
	@touch 
endif	

run_tests: run.exe testInput_TTree.root testInput_TTree_int.root testInput_RNTuple_coll.root testInput_RNTuple_vec.root testInput.arrow
	./run.exe w3piExample2022 loose testInput_TTree.root -f tree
	./run.exe w3piExample2022 loose testInput_RNTuple_coll.root -f rntuple_coll
	./run.exe w3piExample2022 loose testInput_RNTuple_vec.root -f rntuple_vec
	./run.exe w3piExample2022 loose testInput_TTree_int.root -f tree_int
ifdef ARROW_INCLUDE
	./run.exe w3piExample2022 loose testInput.arrow -f arrow_stream
endif

run_tests_output: run.exe testInput_TTree.root testInput_TTree_int.root testInput_RNTuple_coll.root testInput_RNTuple_vec.root
	./run.exe w3piExample2022 loose testInput_TTree.root -f tree -o snapshot snapshot.root
	./run.exe w3piExample2022 loose testInput_TTree_int.root -f tree_int -o snapshot snapshot_int.root
	./run.exe w3piExample2022 loose testInput_TTree.root -f tree -o histo histo.root
	./run.exe w3piExample2022 loose testInput_TTree.root -f tree -o rawhisto histo.raw
	./run.exe w3piExample2022 loose testInput_RNTuple_coll.root -f rntuple_coll -o snapshot snapshot_fromRN.root
	./run.exe w3piExample2022 loose testInput_RNTuple_vec.root -f rntuple_vec -o snapshot snapshot_fromRNV.root
	./run.exe w3piExample2022 loose testInput_RNTuple_coll.root -f rntuple_coll -o histo histo_fromRN.root
	./run.exe w3piExample2022 loose testInput_RNTuple_coll.root -f rntuple_coll -o rawhisto histo_fromRN.raw


DATAFILE ?= ../data/Puppi.dump
TREEFILE = $(OUTDIR)/ttree.root
TREE24FILE = $(OUTDIR)/ttree_24.root
TREEINTFILE = $(OUTDIR)/ttree_int.root
RNTCFILE = $(OUTDIR)/rnt_coll.root
RNTVFILE = $(OUTDIR)/rnt_vec.root
RNTVIFILE = $(OUTDIR)/rnt_vec_int.root
ARRFILE = $(OUTDIR)/ipc.float.arrow
OUTDIR ?= /run/user/$$UID

run_test_speed: ./run.exe
	@echo "=== Create inputs ==="
	../rootUnpacker.exe puppi ttree float $(DATAFILE) $(TREEFILE) > /dev/null
	../rootUnpacker.exe puppi ttree float24 $(DATAFILE) $(TREE24FILE) > /dev/null
	../rootUnpacker.exe puppi ttree int $(DATAFILE) $(TREEINTFILE) > /dev/null
	../rootUnpacker.exe puppi rntuple coll_float $(DATAFILE) $(RNTCFILE) > /dev/null
	../rootUnpacker.exe puppi rntuple floats $(DATAFILE) $(RNTVFILE) > /dev/null
	../rootUnpacker.exe puppi rntuple ints $(DATAFILE) $(RNTVIFILE) > /dev/null
ifdef ARROW_INCLUDE
	../../apache/apacheUnpacker.exe puppi ipcstream float $(DATAFILE) $(ARRFILE) > /dev/null
endif	
	@echo "=== W->3pi 2022 NO OUTPUT ==="
	./run.exe w3piExample2022 loose $(TREEFILE) -f tree 
	./run.exe w3piExample2022 loose $(TREE24FILE) -f tree 
	./run.exe w3piExample2022 loose $(TREEINTFILE) -f tree_int 
	./run.exe w3piExample2022 loose $(RNTCFILE) -f rntuple_coll
	./run.exe w3piExample2022 loose $(RNTVFILE) -f rntuple_vec
	./run.exe w3piExample2022 loose $(RNTVIFILE) -f rntuple_vec_int
ifdef ARROW_INCLUDE
	./run.exe w3piExample2022 loose $(ARRFILE) -f arrow_stream
endif	
	@echo "=== W->3pi 2022 RAW HISTO ==="
	./run.exe w3piExample2022 loose $(TREEFILE) -f tree -o rawhisto histo.raw
	./run.exe w3piExample2022 loose $(RNTCFILE) -f rntuple_coll -o rawhisto histo_fromRN.raw
	./run.exe w3piExample2022 loose $(RNTVFILE) -f rntuple_vec -o rawhisto histo_fromRNv.raw
	@echo "=== W->3pi 2022 SNAPSHOT ==="
	./run.exe w3piExample2022 loose $(TREEFILE) -f tree -o snapshot $(OUTDIR)/snapshot.root
	./run.exe w3piExample2022 loose $(TREE24FILE) -f tree   -o snapshot $(OUTDIR)/snapshot24.root
	./run.exe w3piExample2022 loose $(TREEINTFILE) -f tree_int  -o snapshot $(OUTDIR)/snapshotInt.root
	./run.exe w3piExample2022 loose $(RNTCFILE) -f rntuple_coll -o snapshot $(OUTDIR)/snapshot_fromRN.root
	./run.exe w3piExample2022 loose $(RNTVFILE) -f rntuple_vec -o snapshot $(OUTDIR)/snapshot_fromRNv.root
	./run.exe w3piExample2022 loose $(RNTVIFILE) -f rntuple_vec_int -o snapshot $(OUTDIR)/snapshot_fromRNv.root
ifdef ARROW_INCLUDE
	./run.exe w3piExample2022 loose $(ARRFILE) -f arrow_stream -o snapshot $(OUTDIR)/snapshot_fromArr.root
endif	

