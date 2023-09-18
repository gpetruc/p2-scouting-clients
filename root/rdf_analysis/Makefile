CC = c++
CCFLAGS = -ggdb --std=c++17 -W -Wall -Wno-unused-variable -Ofast -march=native   $(shell root-config --cflags)
LIBS = -lstdc++ $(shell root-config --ldflags --libs) -lROOTNTuple -L/cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/lib64 

.PHONY: clean env format 

all: run.exe

ANALYSES_SRC := analysis.cc w3piExample2022.cc
ANALYSES_OBJ := $(ANALYSES_SRC:.cc=.o)
ANALYSES_H := $(ANALYSES_SRC:.cc=.h)

$(ANALYSES_OBJ): %.o: %.cc %.h analysis.h
	$(CC) $(CCFLAGS) -c $< -o $@

run.exe: run.cc $(ANALYSES_OBJ)
	$(CC) $(CCFLAGS) $(LIBS) $^ -o $@

env:
	@echo "source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/setup.sh"

envdev:
	@echo "source /cvmfs/sft.cern.ch/lcg/views/dev3cuda/latest/x86_64-centos8-gcc11-opt/setup.sh"

format:
	@clang-format -i *.cc

clean:
	@rm *.exe *.root  2> /dev/null || true

testInput_TTree.root:
	../treeUnpacker.exe float ../data/SingleNeutrino.dump  testInput_TTree.root

testInput_TTree_int.root:
	../treeUnpacker.exe int ../data/SingleNeutrino.dump  testInput_TTree_int.root

testInput_RNTuple_coll.root:
	../rntupleUnpacker.exe coll_float ../data/SingleNeutrino.dump  testInput_RNTuple_coll.root

testInput_RNTuple_vec.root:
	../rntupleUnpacker.exe floats ../data/SingleNeutrino.dump  testInput_RNTuple_vec.root

run_tests: run.exe testInput_TTree.root testInput_TTree_int.root testInput_RNTuple_coll.root testInput_RNTuple_vec.root
	./run.exe w3piExample2022 loose testInput_TTree.root -f tree
	./run.exe w3piExample2022 loose testInput_RNTuple_coll.root -f rntuple_coll
	./run.exe w3piExample2022 loose testInput_RNTuple_vec.root -f rntuple_vec
	./run.exe w3piExample2022 loose testInput_TTree_int.root -f tree_int

run_tests_output: run.exe testInput_TTree.root testInput_TTree_int.root testInput_RNTuple_coll.root testInput_RNTuple_vec.root
	./run.exe w3piExample2022 loose testInput_TTree.root -f tree -o snapshot snapshot.root
	./run.exe w3piExample2022 loose testInput_TTree_int.root -f tree_int -o snapshot snapshot_int.root
	./run.exe w3piExample2022 loose testInput_TTree.root -f tree -o histo histo.root
	./run.exe w3piExample2022 loose testInput_TTree.root -f tree -o rawhisto histo.raw
	./run.exe w3piExample2022 loose testInput_RNTuple_coll.root -f rntuple_coll -o snapshot snapshot_fromRN.root
	./run.exe w3piExample2022 loose testInput_RNTuple_vec.root -f rntuple_vec -o snapshot snapshot_fromRNV.root
	./run.exe w3piExample2022 loose testInput_RNTuple_coll.root -f rntuple_coll -o histo histo_fromRN.root
	./run.exe w3piExample2022 loose testInput_RNTuple_coll.root -f rntuple_coll -o rawhisto histo_fromRN.raw


DATAFILE ?= data/SingleNeutrino.dump
TREEFILE ?= /run/user/$$UID/ttree.root
TREE24FILE ?= /run/user/$$UID/ttree_24.root
TREEINTFILE ?= /run/user/$$UID/ttree_int.root
RNTCFILE ?= /run/user/$$UID/rnt_coll.root
RNTVFILE ?= /run/user/$$UID/rnt_vec.root

run_test_speed: ./run.exe
	@echo "=== Create inputs ==="
	../treeUnpacker.exe float $(DATAFILE) $(TREEFILE) > /dev/null
	../treeUnpacker.exe float24 $(DATAFILE) $(TREE24FILE) > /dev/null
	../treeUnpacker.exe int $(DATAFILE) $(TREEINTFILE) > /dev/null
	../rntupleUnpacker.exe coll_float $(DATAFILE) $(RNTCFILE) > /dev/null
	../rntupleUnpacker.exe floats $(DATAFILE) $(RNTVFILE) > /dev/null
	@echo "=== W->3pi 2022 NO OUTPUT ==="
	./run.exe w3piExample2022 loose $(TREEFILE) -f tree 
	./run.exe w3piExample2022 loose $(TREE24FILE) -f tree 
	./run.exe w3piExample2022 loose $(TREEINTFILE) -f tree_int 
	./run.exe w3piExample2022 loose $(RNTCFILE) -f rntuple_coll
	./run.exe w3piExample2022 loose $(RNTVFILE) -f rntuple_vec
	@echo "=== W->3pi 2022 RAW HISTO ==="
	./run.exe w3piExample2022 loose $(TREEFILE) -f tree -o rawhisto histo.raw
	./run.exe w3piExample2022 loose $(RNTCFILE) -f rntuple_coll -o rawhisto histo_fromRN.raw
	./run.exe w3piExample2022 loose $(RNTVFILE) -f rntuple_vec -o rawhisto histo_fromRNv.raw
	@echo "=== W->3pi 2022 SNAPSHOT ==="
	./run.exe w3piExample2022 loose $(TREEFILE) -f tree -o snapshot /run/user/$$UID/snapshot.root
	./run.exe w3piExample2022 loose $(TREE24FILE) -f tree   -o snapshot /run/user/$$UID/snapshot24.root
	./run.exe w3piExample2022 loose $(TREEINTFILE) -f tree_int  -o snapshot /run/user/$$UID/snapshotInt.root
	./run.exe w3piExample2022 loose $(RNTCFILE) -f rntuple_coll -o snapshot /run/user/$$UID/snapshot_fromRN.root
	./run.exe w3piExample2022 loose $(RNTVFILE) -f rntuple_vec -o snapshot /run/user/$$UID/snapshot_fromRNv.root

