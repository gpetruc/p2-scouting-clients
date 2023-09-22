CC = c++
CCFLAGS = --std=c++17 -W -Wall -Wno-unused-variable -Ofast -march=native -Wno-unused-result
LIBS = -L/cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/lib64 -lstdc++ -larrow 
DATAFILE ?= ../root/data/Puppi.dump

.PHONY: clean env format run_test_unpack run_test_speed run_test_comp

all: arrow.exe arrow_read.exe

%.exe : %.cc ../root/unpack.h
	$(CC) $(CCFLAGS) $(LIBS) $< -o $@
env:
	@echo "source /cvmfs/sft.cern.ch/lcg/views/LCG_104/x86_64-centos8-gcc11-opt/setup.sh"

format:
	clang-format -i arrow.cc

clean:
	@rm *.exe *.arrow 2> /dev/null || true


run_test_speed: arrow.exe
	@echo "==== No output ===="
	@./arrow.exe ipc_float $(DATAFILE)
	@./arrow.exe ipc_float_bulk $(DATAFILE)
	@./arrow.exe ipc_float16_bulk $(DATAFILE)
	@./arrow.exe ipc_int_bulk $(DATAFILE) 
	@./arrow.exe ipc_raw64_bulk $(DATAFILE)
	@echo "==== With output ===="
	@./arrow.exe ipc_float16 $(DATAFILE) /run/user/$$UID/out.arrow
	@./arrow.exe ipc_float $(DATAFILE) /run/user/$$UID/out.arrow
	@./arrow.exe ipc_float_bulk $(DATAFILE) /run/user/$$UID/out.arrow
	@./arrow.exe ipc_float16_bulk $(DATAFILE) /run/user/$$UID/out.arrow
	@./arrow.exe ipc_int_bulk $(DATAFILE) /run/user/$$UID/out.arrow
	@./arrow.exe ipc_raw64_bulk $(DATAFILE) /run/user/$$UID/out.arrow

run_test_comp: arrow.exe
	@echo "==== With compressed output ===="
	@./arrow.exe ipc_float -j 6 $(DATAFILE) /run/user/$$UID/out.arrow -z lz4,4
	@./arrow.exe ipc_float_bulk  -j 6 $(DATAFILE) /run/user/$$UID/out.arrow -z lz4,4
	@./arrow.exe ipc_float16_bulk  -j 6 $(DATAFILE) /run/user/$$UID/out.arrow -z lz4,4
	@./arrow.exe ipc_int_bulk  -j 6 $(DATAFILE) /run/user/$$UID/out.arrow -z lz4,4
	@./arrow.exe ipc_raw64_bulk  -j 6 $(DATAFILE) /run/user/$$UID/out.arrow -z lz4,4
	#@echo ""==== LCG libarrow is built without zstd ====""
	#@./arrow.exe ipc_float -j 6 $(DATAFILE) /run/user/$$UID/out.arrow -z zstd,5
	#@./arrow.exe ipc_float -j 6 $(DATAFILE) /run/user/$$UID/out.arrow -z zstd,5 -b 35640
	#@./arrow.exe ipc_float_bulk -j 6 $(DATAFILE) /run/user/$$UID/out.arrow -z zstd,5 -b 35640

run_test_unpack: arrow.exe
	@for T in ipc_float ipc_float16 ipc_float_bulk ipc_float16_bulk  ipc_int_bulk ipc_raw64_bulk ; do  \
		./arrow.exe $$T ../root/data/Puppi.dump $${T}.arrow; \
	done

run_test_multi: arrow.exe
	@for I in $$(seq 1 6); do cp -v $(DATAFILE) /run/user/$$UID/in$${I}.dump; done
	@./arrow.exe ipc_float_bulk /run/user/$$UID/in{1,2,3,4,5,6}.dump;
	@./arrow.exe ipc_float_bulk /run/user/$$UID/in{1,2,3,4,5,6}.dump;
	@./arrow.exe ipc_float_bulk /run/user/$$UID/in{1,2,3,4,5,6}.dump  /run/user/$$UID/out.arrow 