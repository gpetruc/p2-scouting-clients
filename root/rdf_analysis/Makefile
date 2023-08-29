CC = c++
CCFLAGS = -ggdb --std=c++17 -W -Wall -Wno-unused-variable -Ofast -march=native   $(shell root-config --cflags)
LIBS = -lstdc++ $(shell root-config --ldflags --libs) -lROOTNTuple

.PHONY: clean env format 

all: w3pi_ex_2022.exe

%.exe : %.cc
	$(CC) $(CCFLAGS) $(LIBS) $< -o $@

env:
	@echo "source /cvmfs/sft.cern.ch/lcg/views/dev4cuda/latest/x86_64-centos8-gcc11-opt/setup.sh"

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
	./w3pi_ex_2022.exe testInput_TTree.root tree
	./w3pi_ex_2022.exe testInput_RNTuple_coll.root rntuple_coll
	./w3pi_ex_2022.exe testInput_RNTuple_vec.root rntuple_vec