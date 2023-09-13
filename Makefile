CC = c++
CCFLAGS = --std=c++17 -march=native -W -Wall  -Ofast -ggdb
LIBS = -lstdc++ -lpthread

.PHONY: clean format run_tests

all: data_checker.exe  data_generator.exe

%.exe : %.cc
	$(CC) $(CCFLAGS) $(LIBS) $< -o $@

format:
	clang-format -i data_checker.cc
	@cd apache && make format
	@cd root && make format

clean:
	@rm *.exe

run_tests: data_checker.exe
	@echo "Running basic file tests"
	./data_checker.exe DTHBasic data/tcp_genx4.data
	./data_checker.exe DTHBasicOA data/tcp_genx4_OA.data
	bash -c "(sleep 1 && cat data/tcp_genx4_OA.data > /dev/tcp/127.0.0.1/8888 &)"
	./data_checker.exe DTHBasicOA 127.0.0.1:8888
	./data_checker.exe DTHBasic256 data/tcp_gen4_dth256.data
	bash -c "(sleep 1 && cat data/tcp_gen4_dth256.data > /dev/tcp/127.0.0.1/8888 &)"
	./data_checker.exe DTHReceive256 127.0.0.1:8888 native64sz.data 1
	./data_checker.exe Native64SZ native64sz.data