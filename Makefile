CC = c++
CCFLAGS = --std=c++17 -march=native -W -Wall  -O3
LIBS = -lstdc++ -lpthread

.PHONY: clean run_tests

all: data_checker.exe 

%.exe : %.cc
	$(CC) $(CCFLAGS) $(LIBS) $< -o $@

clean:
	@rm *.exe

run_tests: data_checker.exe
	@echo "Running basic file tests"
	./data_checker.exe DTHBasic data/tcp_genx4.data
	./data_checker.exe DTHBasicOA data/tcp_genx4_OA.data
	bash -c "(sleep 1 && cat data/tcp_genx4_OA.data > /dev/tcp/127.0.0.1/8888 &)"
	./data_checker.exe DTHBasicOA 127.0.0.1:8888