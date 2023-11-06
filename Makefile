CC = c++
CCFLAGS = --std=c++17 -march=native -W -Wall  -Ofast -ggdb
LIBS = -lstdc++ -pthread
.PHONY: clean format run_tests env104 envdev env9dev
USE_ROOT ?= 1
USE_APACHE ?= 1

TARGETS := data_checker.exe  data_generator.exe libunpackerBase.so
SUBS :=

ifeq ($(USE_ROOT), 1)
	TARGETS += root/librootUnpacker.so root/rootUnpacker.exe
	CCFLAGS += -DUSE_ROOT=1
endif

ifeq ($(USE_APACHE), 1)
	TARGETS += apache/libapacheUnpacker.so apache/apacheUnpacker.exe
	CCFLAGS += -DUSE_APACHE=1
endif

all: $(TARGETS)

env104:
	@echo 'source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/setup.sh;'
	@echo 'export TBB=/cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt;'
	@echo 'export LD_LIBRARY_PATH=$$PWD:$${LD_LIBRARY_PATH};'
ifeq ($(USE_ROOT), 1)
	@echo 'export LD_LIBRARY_PATH=$$PWD/root:$${LD_LIBRARY_PATH};'
endif
ifeq ($(USE_APACHE), 1)
	@echo 'export ARROW_INCLUDE=/cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/include;'
	@echo 'export ARROW_LIB=/cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/lib64;'
	@echo 'export LD_LIBRARY_PATH=$$PWD/apache:$${LD_LIBRARY_PATH};'
endif

envdev:
	@echo 'source /cvmfs/sft.cern.ch/lcg/views/dev3cuda/latest/x86_64-el8-gcc11-opt/setup.sh;'
	@echo 'export TBB=/cvmfs/sft.cern.ch/lcg/views/dev3cuda/latest/x86_64-el8-gcc11-opt;'
	@echo 'export LD_LIBRARY_PATH=$$PWD:$${LD_LIBRARY_PATH};'
ifeq ($(USE_ROOT), 1)
	@echo 'export LD_LIBRARY_PATH=$$PWD/root:$${LD_LIBRARY_PATH};'
endif
ifeq ($(USE_APACHE), 1)
	@echo 'export ARROW_INCLUDE=/cvmfs/sft.cern.ch/lcg/views/dev3cuda/latest/x86_64-el8-gcc11-opt;'
	@echo 'export ARROW_LIB=/cvmfs/sft.cern.ch/lcg/views/dev3cuda/latest/x86_64-el8-gcc11-opt/lib64;'
	@echo 'export LD_LIBRARY_PATH=$$PWD/apache:$${LD_LIBRARY_PATH};'
endif

env9dev:
	@echo 'source /cvmfs/sft.cern.ch/lcg/views/dev3/latest/x86_64-el9-gcc13-opt/setup.sh;'
	@echo 'export TBB=/cvmfs/sft.cern.ch/lcg/views/dev3/latest/x86_64-el9-gcc13-opt;'
	@echo 'export LD_LIBRARY_PATH=$$PWD:$${LD_LIBRARY_PATH};'
ifeq ($(USE_ROOT), 1)
	@echo 'export LD_LIBRARY_PATH=$$PWD/root:$${LD_LIBRARY_PATH};'
endif
ifeq ($(USE_APACHE), 1)
	@echo 'export ARROW_INCLUDE=/cvmfs/sft.cern.ch/lcg/views/dev3/latest/x86_64-el9-gcc13-opt/include;'
	@echo 'export ARROW_LIB=/cvmfs/sft.cern.ch/lcg/views/dev3/latest/x86_64-el9-gcc13-opt/lib64;'
	@echo 'export LD_LIBRARY_PATH=$$PWD/apache:$${LD_LIBRARY_PATH};'
endif

root/librootUnpacker.so:
	cd root && $(MAKE) librootUnpacker.so
root/rootUnpacker.exe:
	cd root && $(MAKE) rootUnpacker.exe

apache/libapacheUnpacker.so:
	cd apache && $(MAKE) libapacheUnpacker.so
apache/apacheUnpacker.exe:
	cd apache && $(MAKE) apacheUnpacker.exe

UnpackerBase.o: UnpackerBase.cc unpack.h UnpackerBase.h
	$(CC) -fPIC $(CCFLAGS) -c $< -o $@

libunpackerBase.so: UnpackerBase.o
	$(CC) $(CCFLAGS) $^ -shared $(LIBS) -o $@

%.exe : %.cc
	$(CC) $(CCFLAGS) $< $(LIBS) -o $@

receive256tbb.exe : receive256tbb.cc
	$(CC) $(CCFLAGS) $< $(LIBS) -o $@ -I$(TBB)/include -L$(TBB)/lib -ltbb
 
format:
	clang-format -i data_checker.cc data_generator.cc unpack.h UnpackerBase.h
	@cd apache && $(MAKE) format
	@cd root && $(MAKE) format

clean:
	@rm *.exe *.data *.o *.so 2> /dev/null || true
ifeq ($(USE_ROOT), 1)
	@cd root && $(MAKE) clean > /dev/null
endif
ifeq ($(USE_APACHE), 1)
	@cd apache && $(MAKE) clean > /dev/null
endif

run_tests: data_checker.exe data_generator.exe
	@echo "Running basic file tests"
	./data_checker.exe DTHBasic data/tcp_genx4.data
	./data_checker.exe DTHBasicOA data/tcp_genx4_OA.data
	bash -c "(sleep 1 && cat data/tcp_genx4_OA.data > /dev/tcp/127.0.0.1/9988 &)"
	./data_checker.exe DTHBasicOA 127.0.0.1:9988
	./data_checker.exe DTHBasic256 data/tcp_gen4_dth256.data
	bash -c "(sleep 1 && cat data/tcp_gen4_dth256.data > /dev/tcp/127.0.0.1/9988 &)"
	./data_checker.exe DTHReceive256 127.0.0.1:9988 native64sz.data
	./data_checker.exe Native64SZ native64sz.data
	./data_generator.exe Native64 root/data/Puppi.dump native64sz.data --orbits 100 && ./data_checker.exe Native64 native64sz.data
	./data_generator.exe DTHBasic256 root/data/Puppi.dump dth256.data --orbits 100 && ./data_checker.exe DTHBasic256 dth256.data
	bash -c "(sleep 1 && ./data_generator.exe DTHBasic256 root/data/Puppi.dump 127.0.0.1:9988 --orbits 1000 -n 2 --sync &)"
	./data_checker.exe DTHBasic256 127.0.0.1:9988 -n 2