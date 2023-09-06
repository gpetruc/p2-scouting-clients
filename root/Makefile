CC = c++
CCFLAGS = -W -Wall -Wno-unused-variable -Ofast -march=native   $(shell root-config --cflags)
LIBS = -lstdc++ $(shell root-config --ldflags --libs) -lROOTNTuple
DATAFILE ?= data/SingleNeutrino.dump

.PHONY: clean env format run_test_unpack run_test_speed run_test_comp

all: unpack.exe rntuple_unpack.exe

MyDict.cxx: puppi.h Linkdef.h
	rootcling -f $@ $^

%.exe : %.cc unpack.h
	$(CC) $(CCFLAGS) $(LIBS) $< -o $@

rntuple_unpack.exe: rntuple_unpack.cc unpack.h MyDict.cxx
	$(CC) $(CCFLAGS) $(LIBS) $< MyDict.cxx -o $@

env:
	@echo "source /cvmfs/sft.cern.ch/lcg/views/dev4cuda/latest/x86_64-centos8-gcc11-opt/setup.sh"

format:
	clang-format -i puppi.h unpack.h unpack.cc rntuple_unpack.cc

clean:
	@rm *.exe *.root  MyDict.cxx MyDict_rdict.pcm 2> /dev/null || true


run_test_speed: unpack.exe rntuple_unpack.exe
	@echo "==== No output ===="
	@./unpack.exe combined raw64 $(DATAFILE)
	@./unpack.exe combined int $(DATAFILE)
	@./unpack.exe combined float $(DATAFILE)
	@./rntuple_unpack.exe combined raw64 $(DATAFILE)
	@./rntuple_unpack.exe combined float $(DATAFILE)
	@./rntuple_unpack.exe combined_coll float $(DATAFILE)
	@./rntuple_unpack.exe combined_struct float $(DATAFILE)
	@echo "==== With output ===="
	@./unpack.exe combined raw64 $(DATAFILE)  /run/user/$$UID/out.root
	@./unpack.exe combined int $(DATAFILE) /run/user/$$UID/out.root
	@./unpack.exe combined float $(DATAFILE) /run/user/$$UID/out.root
	@./unpack.exe combined float24 $(DATAFILE) /run/user/$$UID/out.root
	@./rntuple_unpack.exe combined raw64 $(DATAFILE)  /run/user/$$UID/out.root
	@./rntuple_unpack.exe combined float $(DATAFILE)  /run/user/$$UID/out.root
	@./rntuple_unpack.exe combined_coll float $(DATAFILE)  /run/user/$$UID/out.root
	@./rntuple_unpack.exe combined_struct float $(DATAFILE)  /run/user/$$UID/out.root

run_test_comp: unpack.exe rntuple_unpack.exe
	@echo "==== With LZ4 output ===="
	@./unpack.exe -j 6 combined float24 $(DATAFILE) /run/user/$$UID/out.root -z lz4,4 
	@./unpack.exe -j 6 combined float $(DATAFILE) /run/user/$$UID/out.root -z lz4,4
	@./unpack.exe -j 6 combined float $(DATAFILE) /run/user/$$UID/out.root -z zstd,5
	@./rntuple_unpack.exe -j 6 combined float $(DATAFILE)  /run/user/$$UID/out.root lz4 4
	@./rntuple_unpack.exe -j 6 combined_coll float $(DATAFILE)  /run/user/$$UID/out.root -z lz4,4
	@./rntuple_unpack.exe -j 6 combined_coll float $(DATAFILE)  /run/user/$$UID/out.root -z zstd,5
	@./rntuple_unpack.exe -j 6 combined_struct float $(DATAFILE)  /run/user/$$UID/out.root -z lz4,4

run_test_unpack: unpack.exe
	@for T in float float24 int; do  \
		for M in separate combined; do  \
			./unpack.exe $$M $$T data/SingleNeutrino.dump; \
			./unpack.exe $$M $$T data/SingleNeutrino.dump unpack_$${T}_$${M}.root; \
		done; \
	done
	@./unpack.exe combined raw64 data/SingleNeutrino.dump; \
	@./unpack.exe combined raw64 data/SingleNeutrino.dump unpack_$${T}_$${M}.root; \

run_test_multi: unpack.exe rntuple_unpack.exe
	@for I in $$(seq 1 6); do cp -v $(DATAFILE) /run/user/$$UID/in$${I}.dump; done
	@./unpack.exe combined float /run/user/$$UID/in{1,2,3,4,5,6}.dump;
	@./unpack.exe combined float /run/user/$$UID/in{1,2,3,4,5,6}.dump;
	@./rntuple_unpack.exe combined_coll float /run/user/$$UID/in{1,2,3,4,5,6}.dump
	@./unpack.exe combined float  /run/user/$$UID/in{1,2,3,4,5,6}.dump /run/user/$$UID/out.root
	@./rntuple_unpack.exe combined_coll float /run/user/$$UID/in{1,2,3,4,5,6}.dump /run/user/$$UID/out.root