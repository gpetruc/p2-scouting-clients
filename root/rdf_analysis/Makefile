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
	EXTRA_HEADERS += RArrowDS2.hxx puppiRawArrowSource.h
	EXTRA_OBJS += RArrowDS2.o puppiRawArrowSource.o
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

puppiRawArrowSource.o : puppiRawArrowSource.cc puppiRawArrowSource.h RArrowDS2.hxx
	$(CC) $(CCFLAGS) -c $< -o $@	
endif

$(ANALYSES_OBJ): %.o: %.cc %.h analysis.h $(EXTRA_HEADERS)
	$(CC) $(CCFLAGS) -c $< -o $@

w3piExample2022Raw.o: w3piExample2022Raw.cc w3piExample2022Raw.h analysis.h w3piExample2022.h
	$(CC) $(CCFLAGS) -c $< -o $@
EXTRA_OBJS += w3piExample2022Raw.o

run.exe: run.cc $(ANALYSES_OBJ) $(EXTRA_OBJS)
	$(CC) $(CCFLAGS) $^ $(LIBS) -o $@

liveAnalysis.exe: liveAnalysis.cc $(ANALYSES_OBJ) $(EXTRA_OBJS)
	$(CC) $(CCFLAGS) $^ $(LIBS) -I$(LCG)/include -ltbb -o $@

testRArrowDS2.exe: testRArrowDS2.cc RArrowDS2.o
	$(CC) $(CCFLAGS) -Wno-unused-result $^ $(LIBS) -I$(LCG)/include -o $@

format:
	@clang-format -i *.cc *.h *.cxx *.hxx

clean:
	@rm *.o *.exe *.root *.raw *.arrow  2> /dev/null || true

testInput_TTree.root:
	../rootUnpacker.exe puppi ttree float ../data/Puppi.dump  testInput_TTree.root > /dev/null

testInput_TTree_int.root:
	../rootUnpacker.exe puppi ttree int ../data/Puppi.dump  testInput_TTree_int.root > /dev/null

testInput_TTree_raw64.root:
	../rootUnpacker.exe puppi ttree raw64 ../data/Puppi.dump  testInput_TTree_raw64.root > /dev/null

TEST_INPUTS := testInput_TTree.root testInput_TTree_int.root testInput_TTree_raw64.root

testInput_RNTuple_coll.root:
	../rootUnpacker.exe puppi rntuple coll_float ../data/Puppi.dump  testInput_RNTuple_coll.root > /dev/null

testInput_RNTuple_vec.root:
	../rootUnpacker.exe puppi rntuple floats ../data/Puppi.dump  testInput_RNTuple_vec.root > /dev/null

testInput_RNTuple_vec_int.root:
	../rootUnpacker.exe puppi rntuple ints ../data/Puppi.dump  testInput_RNTuple_vec_int.root > /dev/null

testInput_RNTuple_vec_raw64.root:
	../rootUnpacker.exe puppi rntuple raw64 ../data/Puppi.dump  testInput_RNTuple_vec_raw64.root > /dev/null

TEST_INPUTS += testInput_RNTuple_coll.root testInput_RNTuple_vec.root testInput_RNTuple_vec_int.root testInput_RNTuple_vec_raw64.root

ifdef ARROW_INCLUDE
testInput.arrow:
	../../apache/apacheUnpacker.exe puppi ipcstream float ../data/Puppi.dump testInput.arrow > /dev/null
testInput_int.arrow:
	../../apache/apacheUnpacker.exe puppi ipcstream int ../data/Puppi.dump testInput_int.arrow > /dev/null
testInput_raw64.arrow:
	../../apache/apacheUnpacker.exe puppi ipcstream raw64 ../data/Puppi.dump testInput_raw64.arrow > /dev/null

TEST_INPUTS += testInput.arrow testInput_int.arrow testInput_raw64.arrow
endif	

run_tests: run.exe $(TEST_INPUTS)
	./run.exe w3piExample2022 loose testInput_TTree.root -f tree -o histo histo.root
	./run.exe w3piExample2022 loose testInput_TTree.root -f tree -o rawhisto histo.raw
	./run.exe w3piExample2022 loose testInput_RNTuple_coll.root -f rntuple_coll -o histo histo_fromRN.root
	./run.exe w3piExample2022 loose testInput_RNTuple_coll.root -f rntuple_coll -o rawhisto histo_fromRN.raw
	./run.exe w3piExample2022 loose testInput_TTree.root -f tree -o snapshot snapshot.root
	./run.exe w3piExample2022 loose testInput_TTree_int.root -f tree_int -o snapshot snapshot_int.root
	./run.exe w3piExample2022 loose testInput_TTree_raw64.root -f tree_raw64 -o snapshot snapshot_raw64.root
	./run.exe w3piExample2022 loose testInput_RNTuple_coll.root -f rntuple_coll -o snapshot snapshot_coll.root
	./run.exe w3piExample2022 loose testInput_RNTuple_vec.root -f rntuple_vec -o snapshot snapshot_vec.root
	./run.exe w3piExample2022 loose testInput_RNTuple_vec_int.root -f rntuple_vec_int -o snapshot snapshot_vec_int.root
	./run.exe w3piExample2022 loose testInput_RNTuple_vec_raw64.root -f rntuple_vec_raw64 -o snapshot snapshot_vec_raw64.root
ifdef ARROW_INCLUDE
	./run.exe w3piExample2022 loose testInput.arrow -f arrow_stream  -o snapshot snapshot_arrow.root
	./run.exe w3piExample2022 loose testInput_int.arrow -f arrow_stream_int   -o snapshot snapshot_arrow_int.root
	./run.exe w3piExample2022 loose testInput_raw64.arrow -f arrow_stream_raw64   -o snapshot snapshot_arrow_raw64.root
	./run.exe w3piExample2022 loose ../data/Puppi.dump -f arrow_raw_raw64   -o snapshot snapshot_arrow_raw_raw64.root
endif
	./run.exe w3piExample2022Raw loose ../data/Puppi.dump  -o snapshot snapshot_raw.root

DATAFILE ?= ../data/Puppi.dump
TREEFILE = $(OUTDIR)/ttree.root
TREE24FILE = $(OUTDIR)/ttree_24.root
TREEINTFILE = $(OUTDIR)/ttree_int.root
TREE64FILE = $(OUTDIR)/ttree_raw64.root
RNTCFILE = $(OUTDIR)/rnt_coll.root
RNTVFILE = $(OUTDIR)/rnt_vec.root
RNTVIFILE = $(OUTDIR)/rnt_vec_int.root
RNTV64FILE = $(OUTDIR)/rnt_raw64.root
ARRFILE = $(OUTDIR)/ipc.arrow
OUTDIR ?= /run/user/$$UID

run_test_speed: ./run.exe
	@echo "=== Making float inputs ==="
	../rootUnpacker.exe puppi ttree float $(DATAFILE) $(TREEFILE) > /dev/null
	../rootUnpacker.exe puppi ttree float24 $(DATAFILE) $(TREE24FILE) > /dev/null
	../rootUnpacker.exe puppi rntuple coll_float $(DATAFILE) $(RNTCFILE) > /dev/null
	../rootUnpacker.exe puppi rntuple floats $(DATAFILE) $(RNTVFILE) > /dev/null
ifdef ARROW_INCLUDE
	../../apache/apacheUnpacker.exe puppi ipcstream float $(DATAFILE) $(ARRFILE) > /dev/null
endif	
	@echo "=== W->3pi 2022 NO OUTPUT ==="
	./run.exe w3piExample2022 loose $(TREEFILE) -f tree 
	./run.exe w3piExample2022 loose $(TREE24FILE) -f tree 
	./run.exe w3piExample2022 loose $(RNTCFILE) -f rntuple_coll
	./run.exe w3piExample2022 loose $(RNTVFILE) -f rntuple_vec
ifdef ARROW_INCLUDE
	./run.exe w3piExample2022 loose $(ARRFILE) -f arrow_stream
endif	
	@echo "=== W->3pi 2022 RAW HISTO ==="
	./run.exe w3piExample2022 loose $(TREEFILE) -f tree -o $(OUTDIR)/rawhisto histo.raw
	./run.exe w3piExample2022 loose $(RNTCFILE) -f rntuple_coll -o $(OUTDIR)/rawhisto histo_fromRN.raw
	./run.exe w3piExample2022 loose $(RNTVFILE) -f rntuple_vec -o $(OUTDIR)/rawhisto histo_fromRNv.raw
	@echo "=== W->3pi 2022 SNAPSHOT ==="
	./run.exe w3piExample2022 loose $(TREEFILE) -f tree -o snapshot $(OUTDIR)/snapshot.root
	./run.exe w3piExample2022 loose $(TREE24FILE) -f tree   -o snapshot $(OUTDIR)/snapshot24.root
	./run.exe w3piExample2022 loose $(RNTCFILE) -f rntuple_coll -o snapshot $(OUTDIR)/snapshot_fromRN.root
	./run.exe w3piExample2022 loose $(RNTVFILE) -f rntuple_vec -o snapshot $(OUTDIR)/snapshot_fromRNv.root
ifdef ARROW_INCLUDE
	./run.exe w3piExample2022 loose $(ARRFILE) -f arrow_stream -o snapshot $(OUTDIR)/snapshot_fromArr.root
endif
	@rm $(TREE24FILE) $(RNTCFILE)
	@echo "=== Making integer inputs ==="
	../rootUnpacker.exe puppi ttree int $(DATAFILE) $(TREEFILE) > /dev/null
	../rootUnpacker.exe puppi rntuple ints $(DATAFILE) $(RNTVFILE) > /dev/null
ifdef ARROW_INCLUDE
	../../apache/apacheUnpacker.exe puppi ipcstream int $(DATAFILE) $(ARRFILE) > /dev/null
endif	
	@echo "=== W->3pi 2022 SNAPSHOT (integer) ==="
	./run.exe w3piExample2022 loose $(TREEFILE) -f tree_int -o snapshot $(OUTDIR)/snapshot.root
	./run.exe w3piExample2022 loose $(RNTVFILE) -f rntuple_vec_int -o snapshot $(OUTDIR)/snapshot_fromRNv.root
ifdef ARROW_INCLUDE
	./run.exe w3piExample2022 loose $(ARRFILE) -f arrow_stream_int -o snapshot $(OUTDIR)/snapshot_fromArr.root
endif
	@echo "=== Making raw64 inputs ==="
	../rootUnpacker.exe puppi ttree raw64 $(DATAFILE) $(TREEFILE) > /dev/null
	../rootUnpacker.exe puppi rntuple raw64 $(DATAFILE) $(RNTVFILE) > /dev/null
ifdef ARROW_INCLUDE
	../../apache/apacheUnpacker.exe puppi ipcstream raw64 $(DATAFILE) $(ARRFILE) > /dev/null
endif	
	@echo "=== W->3pi 2022 SNAPSHOT (raw64) ==="
	./run.exe w3piExample2022 loose $(TREEFILE) -f tree_raw64 -o snapshot $(OUTDIR)/snapshot.root
	./run.exe w3piExample2022 loose $(RNTVFILE) -f rntuple_vec_raw64 -o snapshot $(OUTDIR)/snapshot_fromRNv.root
ifdef ARROW_INCLUDE
	./run.exe w3piExample2022 loose $(ARRFILE) -f arrow_stream_raw64 -o snapshot $(OUTDIR)/snapshot_fromArr.root
endif
	@echo "=== W->3pi 2022 FROM RAW ==="
	./run.exe w3piExample2022  loose $(DATAFILE) -f arrow_raw_raw64
	./run.exe w3piExample2022Raw loose $(DATAFILE)
	./run.exe w3piExample2022 loose $(DATAFILE) -f arrow_raw_raw64 -o snapshot $(OUTDIR)/snapshot_fromArrRaw.root
	./run.exe w3piExample2022Raw loose $(DATAFILE) -o snapshot $(OUTDIR)/snapshot_fromRaw.root