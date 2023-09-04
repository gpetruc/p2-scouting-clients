CC = c++
CCFLAGS = -ggdb --std=c++17 -W -Wall -Wno-unused-variable -Ofast -march=native   $(shell root-config --cflags)
LIBS = -lstdc++ $(shell root-config --ldflags --libs) -lROOTNTuple -L/cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/lib64 

.PHONY: clean env format 

all: w3pi_ex_2022.exe

%.exe : %.cc
	$(CC) $(CCFLAGS) $(LIBS) $< -o $@

env:
	@echo "source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/setup.sh"

format:
	@clang-format -i *.cc

clean:
	@rm *.exe *.root  2> /dev/null || true


../%.exe: ../%.cc ../Makefile ../puppi.h ../puppi.cc ../unpack.h ../unpack.cc
	@cd .. && make %.exe

testInput_TTree.root: ../unpack.exe
	@../unpack.exe combined float ../data/SingleNeutrino.dump  testInput_TTree.root

testInput_RNTuple_coll.root: ../rntuple_unpack.exe
	@../rntuple_unpack.exe combined_coll float ../data/SingleNeutrino.dump  testInput_RNTuple_coll.root

testInput_RNTuple_vec.root: ../rntuple_unpack.exe
	@../rntuple_unpack.exe combined float ../data/SingleNeutrino.dump  testInput_RNTuple_vec.root

run_tests: w3pi_ex_2022.exe testInput_TTree.root testInput_RNTuple_coll.root testInput_RNTuple_vec.root
	./w3pi_ex_2022.exe testInput_TTree.root -f tree
	./w3pi_ex_2022.exe testInput_RNTuple_coll.root -f rntuple_coll
	./w3pi_ex_2022.exe testInput_RNTuple_vec.root -f rntuple_vec

run_tests_output: w3pi_ex_2022.exe testInput_TTree.root testInput_RNTuple_coll.root
	./w3pi_ex_2022.exe testInput_TTree.root -f tree -o snapshot snapshot.root
	./w3pi_ex_2022.exe testInput_TTree.root -f tree -o histo histo.root
	./w3pi_ex_2022.exe testInput_TTree.root -f tree -o rawhisto histo.raw
	./w3pi_ex_2022.exe testInput_RNTuple_coll.root -f rntuple_coll -o snapshot snapshot_fromRN.root
	./w3pi_ex_2022.exe testInput_RNTuple_coll.root -f rntuple_coll -o histo histo_fromRN.root
	./w3pi_ex_2022.exe testInput_RNTuple_coll.root -f rntuple_coll -o rawhisto histo_fromRN.raw

DATAFILE ?= data/SingleNeutrino.dump
TREEFILE ?= /run/user/$$UID/out.root
RNTCFILE ?= /run/user/$$UID/out_rnt_coll.root

$(TREEFILE): ../unpack.exe
	@../unpack.exe combined float $(DATAFILE) $(TREEFILE)
$(RNTCFILE): ../rntuple_unpack.exe
	@../rntuple_unpack.exe combined_coll float  $(DATAFILE) $(RNTCFILE)

run_test_speed: ./w3pi_ex_2022.exe  $(TREEFILE)  $(RNTCFILE)
	@echo "=== NO OUTPUT ==="
	./w3pi_ex_2022.exe $(TREEFILE) -f tree 
	./w3pi_ex_2022.exe $(RNTCFILE) -f rntuple_coll
	@echo "=== RAW HISTO ==="
	./w3pi_ex_2022.exe $(TREEFILE) -f tree -o rawhisto histo.raw
	./w3pi_ex_2022.exe $(RNTCFILE) -f rntuple_coll -o rawhisto histo_fromRN.raw
	@echo "=== SNAPSHOT ==="
	./w3pi_ex_2022.exe $(TREEFILE) -f tree -o snapshot snapshot.root
	./w3pi_ex_2022.exe $(RNTCFILE) -f rntuple_coll -o snapshot snapshot_fromRN.root

