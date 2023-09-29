CC = c++
CCFLAGS = -ggdb --std=c++17 -W -Wall -Wno-unused-variable -Ofast -march=native -Wno-unused-result
LIBS = -L/cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/lib64 -lstdc++ -larrow 
DATAFILE ?= ../root/data/Puppi.dump
TKMUDATAFILE ?= ../root/data/TkMuons_TM18.dump

.PHONY: clean env format run_test_unpack run_test_speed run_test_comp

all: arrow_read.exe apacheUnpacker.exe

BASE_INC := ../unpack.h  ../UnpackerBase.h
BASE_LIB := ../libunpackerBase.so
../libunpackerBase.so: ../UnpackerBase.cc $(BASE_INC)
	cd .. && $(MAKE) libunpackerBase.so

IPC_UNPACKER_SRC := $(wildcard *IPCUnpacker*.cc)
IPC_UNPACKER_INC := $(IPC_UNPACKER_SRC:.cc=.h)
IPC_UNPACKER_OBJ := $(IPC_UNPACKER_SRC:.cc=.o)
$(IPC_UNPACKER_OBJ): %.o: %.cc %.h IPCUnpackerBase.h $(BASE_INC)
	$(CC) -fPIC $(CCFLAGS) -c $< -o $@

ApacheUnpackMaker.o : ApacheUnpackMaker.cc ApacheUnpackMaker.h  $(IPC_UNPACKER_INC)  $(BASE_INC)
	$(CC) -fPIC $(CCFLAGS) -c $< -o $@

libapacheUnpacker.so: ApacheUnpackMaker.o $(IPC_UNPACKER_OBJ) $(BASE_LIB)
	$(CC) -shared $(CCFLAGS) $(LIBS) $^ -L.. -lunpackerBase -o $@

apacheUnpacker.exe: ApacheUnpackMain.cc libapacheUnpacker.so
	$(CC) $(CCFLAGS) $(LIBS) $^ -L.. -L. -lunpackerBase -lapacheUnpacker -o $@

arrow_read.exe : arrow_read.cc
	$(CC) $(CCFLAGS) $(LIBS) $< -o $@

format:
	@clang-format -i *.h *.cc

clean:
	@rm *.exe *.arrow *.o *.so 2> /dev/null || true

run_tests: apacheUnpacker.exe arrow_read.exe
	@for T in float float16 int raw64 ; do  \
		./apacheUnpacker.exe puppi ipc $$T ../root/data/Puppi.dump puppi.ipc.$${T}.arrow; \
	done
	@for T in float float16 ; do  \
		./apacheUnpacker.exe tkmu ipc $$T ../root/data/TkMuons_TM18.dump tkmu.ipc.$${T}.arrow; \
	done
	@for T in float float16 int raw64 ; do  \
		./arrow_read.exe ipc  puppi.ipc.$${T}.arrow | head -n 15; echo; \
	done

run_test_speed: apacheUnpacker.exe
	@echo "==== No output ===="
	@./apacheUnpacker.exe puppi ipc float $(DATAFILE)
	@./apacheUnpacker.exe puppi ipc float16 $(DATAFILE)
	@./apacheUnpacker.exe puppi ipc int $(DATAFILE) 
	@./apacheUnpacker.exe puppi ipc raw64 $(DATAFILE) 
	@./apacheUnpacker.exe tkmu ipc float $(TKMUDATAFILE)
	@echo "==== With output ===="
	@./apacheUnpacker.exe puppi ipc float $(DATAFILE) /run/user/$$UID/out.arrow
	@./apacheUnpacker.exe puppi ipc float16 $(DATAFILE) /run/user/$$UID/out.arrow
	@./apacheUnpacker.exe puppi ipc int $(DATAFILE) /run/user/$$UID/out.arrow
	@./apacheUnpacker.exe puppi ipc raw64 $(DATAFILE) /run/user/$$UID/out.arrow
	@./apacheUnpacker.exe tkmu ipc float $(TKMUDATAFILE) /run/user/$$UID/out.arrow

run_test_comp: apacheUnpacker.exe
	@echo "==== With compressed output ===="
	@./apacheUnpacker.exe puppi ipc float $(DATAFILE) /run/user/$$UID/out.arrow -j 6 -z lz4,4
	@./apacheUnpacker.exe puppi ipc float16 $(DATAFILE) /run/user/$$UID/out.arrow -j 6 -z lz4,4
	@./apacheUnpacker.exe puppi ipc int $(DATAFILE) /run/user/$$UID/out.arrow -j 6 -z lz4,4
	@./apacheUnpacker.exe puppi ipc raw64 $(DATAFILE) /run/user/$$UID/out.arrow -j 6 -z lz4,4


run_test_multi: arrow.exe
	@for I in $$(seq 1 6); do cp -v $(DATAFILE) /run/user/$$UID/in$${I}.dump; done
	@./apacheUnpacker.exe puppi ipc float /run/user/$$UID/in{1,2,3,4,5,6}.dump;
	@./apacheUnpacker.exe puppi ipc float /run/user/$$UID/in{1,2,3,4,5,6}.dump;
	@./apacheUnpacker.exe puppi ipc float /run/user/$$UID/in{1,2,3,4,5,6}.dump  /run/user/$$UID/out.arrow 