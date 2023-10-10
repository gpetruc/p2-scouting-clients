CC = c++
CCFLAGS = -ggdb --std=c++17 -W -Wall -Wno-unused-variable -Wno-unused-parameter -Wno-redundant-move -Ofast -march=native -Wno-unused-result
LIBS =  -lstdc++ 

ifdef ARROW_INCLUDE
	CCFLAGS += -I$(ARROW_INCLUDE)
endif
ifdef ARROW_LIB
	LIBS += -L$(ARROW_LIB)
endif
LIBS += -larrow

USE_PARQUET ?= 0
ifeq ($(USE_PARQUET), 1)
	CCFLAGS += -DUSE_PARQUET=1
	ifdef PARQUET_INCLUDE
		CCFLAGS += -I$(PARQUET_INCLUDE)
	endif
	ifdef PARQUET_LIB
		LIBS += -L$(PARQUET_LIB)
	endif
	LIBS += -lparquet
endif

DATAFILE ?= ../root/data/Puppi.dump
TKMUDATAFILE ?= ../root/data/TkMuons_TM18.dump
OUTDIR ?= /run/user/$$UID

.PHONY: clean format run_tests run_test_parquet run_test_speed run_test_comp

all: arrow_read.exe apacheUnpacker.exe

vpath %.so . ..
BASE_INC := ../unpack.h  ../UnpackerBase.h
BASE_LIB := libunpackerBase.so
$(BASE_LIB): ../UnpackerBase.cc $(BASE_INC)
	cd .. && $(MAKE) $@

ARROW_UNPACKER_SRC := $(wildcard *ArrowUnpacker*.cc)
ARROW_UNPACKER_INC := $(ARROW_UNPACKER_SRC:.cc=.h)
ARROW_UNPACKER_OBJ := $(ARROW_UNPACKER_SRC:.cc=.o)
$(ARROW_UNPACKER_OBJ): %.o: %.cc %.h ArrowUnpackerBase.h $(BASE_INC)
	$(CC) -fPIC $(CCFLAGS) -c $< -o $@

ALLOBJ := ApacheUnpackMaker.o $(ARROW_UNPACKER_OBJ)

ApacheUnpackMaker.o : ApacheUnpackMaker.cc ApacheUnpackMaker.h  $(ARROW_UNPACKER_INC)  $(BASE_INC)
	$(CC) -fPIC $(CCFLAGS) -c $< -o $@

libapacheUnpacker.so: $(ALLOBJ) -lunpackerBase
	$(CC) -shared $(CCFLAGS) $(LIBS) $(ALLOBJ) -L.. -lunpackerBase -o $@

apacheUnpacker.exe: ApacheUnpackMain.cc -lunpackerBase libapacheUnpacker.so
	$(CC) $(CCFLAGS) $(LIBS) $< -L.. -L. -lunpackerBase -lapacheUnpacker -o $@

arrow_read.exe : arrow_read.cc
	$(CC) $(CCFLAGS) $(LIBS) $< -o $@

format:
	@clang-format -i *.h *.cc

clean:
	@rm *.exe *.arrow *.parquet *.o *.so 2> /dev/null || true

run_tests: apacheUnpacker.exe arrow_read.exe
	./apacheUnpacker.exe puppi ipcstream float ../root/data/Puppi.dump puppi.ipc.float.arrow
	./apacheUnpacker.exe puppi ipcfile float ../root/data/Puppi.dump puppi.ipcfile.float.arrow
	./apacheUnpacker.exe puppi ipcstream float16 ../root/data/Puppi.dump puppi.ipc.float16.arrow
	./apacheUnpacker.exe puppi ipcstream int ../root/data/Puppi.dump puppi.ipc.int.arrow
	./apacheUnpacker.exe puppi ipcstream raw64 ../root/data/Puppi.dump puppi.ipc.raw64.arrow
	./apacheUnpacker.exe tkmu ipcstream float ../root/data/TkMuons_TM18.dump tkmu.ipc.float.arrow; 
	./apacheUnpacker.exe tkmu ipcstream float16 ../root/data/TkMuons_TM18.dump tkmu.ipc.float16.arrow; 
	@for T in float float16 int raw64 ; do  \
		./arrow_read.exe ipcstream  puppi.ipc.$${T}.arrow; echo; \
		[[ "$$T" == "float" ]] && ./arrow_read.exe ipcfile  puppi.ipcfile.$${T}.arrow; echo; \
		break; \
	done

run_test_parquet: apacheUnpacker.exe arrow_read.exe
	./apacheUnpacker.exe puppi parquet float ../root/data/Puppi.dump puppi.parquet.float.parquet
	./apacheUnpacker.exe puppi parquet float16 ../root/data/Puppi.dump puppi.parquet.float16.parquet
	./apacheUnpacker.exe puppi parquet int ../root/data/Puppi.dump puppi.parquet.int.parquet
	./apacheUnpacker.exe puppi parquet raw64 ../root/data/Puppi.dump puppi.parquet.raw64.parquet
	./apacheUnpacker.exe tkmu parquet float ../root/data/TkMuons_TM18.dump tkmu.parquet.float.parquet; 

run_test_speed: apacheUnpacker.exe
	@echo "==== No output ===="
	@./apacheUnpacker.exe puppi ipcstream float $(DATAFILE)
	@./apacheUnpacker.exe puppi ipcstream float16 $(DATAFILE)
	@./apacheUnpacker.exe puppi ipcstream int $(DATAFILE) 
	@./apacheUnpacker.exe puppi ipcstream raw64 $(DATAFILE) 
	@./apacheUnpacker.exe tkmu ipcstream float $(TKMUDATAFILE)
	@echo "==== With output ===="
	@./apacheUnpacker.exe puppi ipcstream float $(DATAFILE) $(OUTDIR)/out.arrow
	@./apacheUnpacker.exe puppi ipcstream float16 $(DATAFILE) $(OUTDIR)/out.arrow
	@./apacheUnpacker.exe puppi ipcstream int $(DATAFILE) $(OUTDIR)/out.arrow
	@./apacheUnpacker.exe puppi ipcstream raw64 $(DATAFILE) $(OUTDIR)/out.arrow
	@./apacheUnpacker.exe tkmu ipcstream float $(TKMUDATAFILE) $(OUTDIR)/out.arrow

run_test_comp: apacheUnpacker.exe
	@echo "==== With compressed output ===="
	@./apacheUnpacker.exe puppi ipcstream float $(DATAFILE) $(OUTDIR)/out.arrow -j 6 -z lz4,4
	@./apacheUnpacker.exe puppi ipcstream float16 $(DATAFILE) $(OUTDIR)/out.arrow -j 6 -z lz4,4
	@./apacheUnpacker.exe puppi ipcstream int $(DATAFILE) $(OUTDIR)/out.arrow -j 6 -z lz4,4
	@./apacheUnpacker.exe puppi ipcstream raw64 $(DATAFILE) $(OUTDIR)/out.arrow -j 6 -z lz4,4


run_test_multi: arrow.exe
	@for I in $$(seq 1 6); do cp -v $(DATAFILE) $(OUTDIR)/in$${I}.dump; done
	@./apacheUnpacker.exe puppi ipcstream float $(OUTDIR)/in{1,2,3,4,5,6}.dump;
	@./apacheUnpacker.exe puppi ipcstream float $(OUTDIR)/in{1,2,3,4,5,6}.dump;
	@./apacheUnpacker.exe puppi ipcstream float $(OUTDIR)/in{1,2,3,4,5,6}.dump  $(OUTDIR)/out.arrow 