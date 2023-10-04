CC = c++
CCFLAGS = -ggdb --std=c++17 -W -Wall -Wno-unused-variable -Ofast -march=native -Wno-unused-result
LIBS =  -lstdc++ 

ifdef ARROW_INCLUDE
	CLAGS += -I$(ARROW_INCLUDE)
endif
ifdef ARROW_LIB
	LIBS += -L$(ARROW_LIB)
endif
LIBS += -larrow

USE_PARQUET ?= 0
ifeq ($(USE_PARQUET), 1)
	CCFLAGS += -DUSE_PARQUET=1
	ifdef PARQUET_INCLUDE
		CLAGS += -I$(PARQUET_INCLUDE)
	endif
	ifdef PARQUET_LIB
		LIBS += -L$(PARQUET_LIB)
	endif
	LIBS += -lparquet
endif

DATAFILE ?= ../root/data/Puppi.dump
TKMUDATAFILE ?= ../root/data/TkMuons_TM18.dump

.PHONY: clean env format run_tests run_test_parquet run_test_speed run_test_comp

all: arrow_read.exe apacheUnpacker.exe

BASE_INC := ../unpack.h  ../UnpackerBase.h
BASE_LIB := ../libunpackerBase.so
../libunpackerBase.so: ../UnpackerBase.cc $(BASE_INC)
	cd .. && $(MAKE) libunpackerBase.so

IPC_UNPACKER_SRC := $(wildcard *ArrowUnpacker*.cc)
IPC_UNPACKER_INC := $(IPC_UNPACKER_SRC:.cc=.h)
IPC_UNPACKER_OBJ := $(IPC_UNPACKER_SRC:.cc=.o)
$(IPC_UNPACKER_OBJ): %.o: %.cc %.h ArrowUnpackerBase.h $(BASE_INC)
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
	@rm *.exe *.arrow *.icp *.parquet *.o *.so 2> /dev/null || true

run_tests: apacheUnpacker.exe arrow_read.exe
	./apacheUnpacker.exe puppi ipc float ../root/data/Puppi.dump puppi.ipc.float.arrow
	./apacheUnpacker.exe puppi ipc float16 ../root/data/Puppi.dump puppi.ipc.float16.arrow
	./apacheUnpacker.exe puppi ipc int ../root/data/Puppi.dump puppi.ipc.int.arrow
	./apacheUnpacker.exe puppi ipc raw64 ../root/data/Puppi.dump puppi.ipc.raw64.arrow
	./apacheUnpacker.exe tkmu ipc float ../root/data/TkMuons_TM18.dump tkmu.ipc.float.arrow; 
	./apacheUnpacker.exe tkmu ipc float16 ../root/data/TkMuons_TM18.dump tkmu.ipc.float16.arrow; 
	@for T in float float16 int raw64 ; do  \
		./arrow_read.exe ipc  puppi.ipc.$${T}.arrow | head -n 15; echo; \
	done

run_test_parquet: apacheUnpacker.exe arrow_read.exe
	./apacheUnpacker.exe puppi parquet float ../root/data/Puppi.dump puppi.parquet.float.parquet
	./apacheUnpacker.exe puppi parquet float16 ../root/data/Puppi.dump puppi.parquet.float16.parquet
	./apacheUnpacker.exe puppi parquet int ../root/data/Puppi.dump puppi.parquet.int.parquet
	./apacheUnpacker.exe puppi parquet raw64 ../root/data/Puppi.dump puppi.parquet.raw64.parquet
	./apacheUnpacker.exe tkmu parquet float ../root/data/TkMuons_TM18.dump tkmu.parquet.float.parquet; 

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