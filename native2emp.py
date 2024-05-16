#!/usr/bin/env python3
from optparse import OptionParser
parser = OptionParser("%(prog) infile [ src [ dst ] ]")
parser.add_option("-t","--tmux", dest="tmux", type=int, default=1)
parser.add_option("--mux-orbits", dest="orbitMux", type=int, default=1)
parser.add_option("--type", dest="dumpType", type=str, default="puppi")
parser.add_option("-n","--nevents", dest="events", type=int, default=18)
parser.add_option("--nmax", dest="nmaxPuppi", type=int, default=208, help="BX per orbit")
parser.add_option("-o","--out", dest="out", type=str, default="dump")
options, args = parser.parse_args()

import sys, random
rnd = random.Random(37)

class PuppiFile:
    def __init__(self, fname):
        self._fname = fname
        if fname != None:
            fin = open(fname,'rb')
            index = []
            sumn = 0; nent = 0
            while fin.readable():
                w64 = fin.read(8)
                if not w64: break
                npuppi = (int.from_bytes(w64, sys.byteorder) & 0xFFF)
                puppidata = fin.read(8*npuppi)
                index.append((npuppi,puppidata))
                sumn += npuppi
                nent += 1
                if nent % 10000 == 0:
                    print("Processed %7d entries" % nent)
            self._index = index
            print("File %s with %d events, avg multiplicity %.2f" % (fname,len(index),sumn/float(len(index))));
    def push(self,lout,rnd):
        if self._fname != None:
            (n,data) = rnd.choice(self._index)
            if (n >= options.nmaxPuppi):
                print("Warning, cropping event with %d candidates" % n)
                n = options.nmaxPuppi
                data = data[:8*options.nmaxPuppi]
            lout.append((n,data))
        else:
            lout.append((0,None))
makers = dict(puppi=PuppiFile)
maker = makers[options.dumpType](args[0])
channels = [int(c) for c in args[1:]]
datasets = [[] for t in range(options.tmux)]
while len(channels) < len(datasets):
    if len(channels) == 0: 
        channels.append(40)
    else:
        channels.append(max(channels)+4)
for bx in range(options.tmux * options.events):
    ibx = bx % options.tmux
    maker.push(datasets[ibx], rnd)
for t, (d, c) in enumerate(zip(datasets,sorted(channels))):    
    print(f"Writing {len(d)} events to {options.out}.ch{c}.txt")
    fout = open(f"{options.out}.ch{c}.txt", "w")
    fout.write("ID: Test\nMetadata: (strobe,) start of orbit, start of packet, end of packet, valid\n\n")
    fout.write(f"     Link              {c+0:03d}                    {c+1:03d}                    {c+2:03d}                    {c+3:03d}\n") 
    frame = 0; orbit = 1
    for iev, (n,data) in enumerate(d):
        for i in range(9*options.tmux):
            start = 1 if i == 0 else 0
            fout.write(f"Frame {frame:04d}    ")    
            for j in range(4):
                valid = 1 if (i == 0 or 4*i+j < n) else 0
                end = 1 if valid and 4*i+j+4 >= n else 0
                w64 = int.from_bytes(data[(4*i+j)*8:(4*i+j)*8+8], byteorder='little') if 4*i+j < n else 0
                fout.write(f"{orbit}{start}{end}{valid} {w64:016x}") 
                if j < 3: 
                    fout.write("  ")
            fout.write("\n")
            frame += 1
            orbit = 0
