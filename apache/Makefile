CC = c++
CCFLAGS = --std=c++17 -W -Wall -Wno-unused-variable -Ofast -march=native 
LIBS = -lstdc++ -larrow 
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
	@echo "==== With output ===="
	@./arrow.exe ipc_float16 $(DATAFILE) /run/user/$$UID/out.arrow
	@./arrow.exe ipc_float $(DATAFILE) /run/user/$$UID/out.arrow
	@./arrow.exe ipc_float_bulk $(DATAFILE) /run/user/$$UID/out.arrow
	@echo "==== With compressed output ===="
	@./arrow.exe ipc_float -j 6 $(DATAFILE) /run/user/$$UID/out.arrow lz4 4
	@./arrow.exe ipc_float_bulk  -j 6 $(DATAFILE) /run/user/$$UID/out.arrow lz4 4
	@./arrow.exe ipc_float -j 6 $(DATAFILE) /run/user/$$UID/out.arrow zstd 5
	@./arrow.exe ipc_float -j 6 $(DATAFILE) /run/user/$$UID/out.arrow zstd 5 -b 35640
	@./arrow.exe ipc_float_bulk -j 6 $(DATAFILE) /run/user/$$UID/out.arrow zstd 5 -b 35640

run_test_unpack: arrow.exe
	@for T in ipc_float ipc_float_bulk ipc_float16; do  \
		./arrow.exe $$T ../root/data/SingleNeutrino.dump $${T}.arrow; \
	done
