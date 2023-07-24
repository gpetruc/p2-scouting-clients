CC = c++
CCFLAGS = --std=c++17 -W -Wall -Wno-unused-variable -Ofast -march=native -Wno-unused-result
LIBS = -L/cvmfs/sft.cern.ch/lcg/views/dev4cuda/latest/x86_64-centos8-gcc11-opt/lib64 -lstdc++ -larrow 
DATAFILE ?= ../root/data/SingleNeutrino.dump

.PHONY: clean 

all: arrow.exe

%.exe : %.cc ../root/unpack.h
	$(CC) $(CCFLAGS) $(LIBS) $< -o $@

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
	@./arrow.exe ipc_float -j 6 $(DATAFILE) /run/user/$$UID/out.arrow lz4 4
	@./arrow.exe ipc_float_bulk  -j 6 $(DATAFILE) /run/user/$$UID/out.arrow lz4 4
	@./arrow.exe ipc_float16_bulk  -j 6 $(DATAFILE) /run/user/$$UID/out.arrow lz4 4
	@./arrow.exe ipc_int_bulk  -j 6 $(DATAFILE) /run/user/$$UID/out.arrow lz4 4
	@./arrow.exe ipc_raw64_bulk  -j 6 $(DATAFILE) /run/user/$$UID/out.arrow lz4 4
	#@echo ""==== LCG libarrow is built without zstd ====""
	#@./arrow.exe ipc_float -j 6 $(DATAFILE) /run/user/$$UID/out.arrow zstd 5
	#@./arrow.exe ipc_float -j 6 $(DATAFILE) /run/user/$$UID/out.arrow zstd 5 -b 35640
	#@./arrow.exe ipc_float_bulk -j 6 $(DATAFILE) /run/user/$$UID/out.arrow zstd 5 -b 35640

run_test_unpack: arrow.exe
	@for T in ipc_float ipc_float16 ipc_float_bulk ipc_float16_bulk  ipc_int_bulk ipc_raw64_bulk ; do  \
		./arrow.exe $$T ../root/data/SingleNeutrino.dump $${T}.arrow; \
	done
