#!/usr/bin/env python3
from optparse import OptionParser
import sys, io

parser = OptionParser("data_checker.py input")
parser.add_option("--orbit-aggregation","--OA", dest="orbitAggregation", default=False, action="store_true", help="DTH was run in orbit aggregation mode")
parser.add_option("--no-npuppi", dest="npuppi", default=True, action="store_false", help="NPuppi filled correctly")
parser.add_option("--firstbx", dest="firstbx", type=int, default=0, help="first bx")
parser.add_option("--tmux", dest="tmux", type=int, default=6, help="tmux factor")
parser.add_option("--nprint", dest="nprint", type=int, default=5, help="events to print")
parser.add_option("--max-events", "-N", dest="maxev", type=int, default=-1, help="max number of events")
parser.add_option("--playLoop", dest="playLoop", default=False, action="store_true", help="Input was in playLoop")
options, args = parser.parse_args()

def getbit(bytes,i):
    return (bytes[i//8] & (1 << (i%8))) != 0
def s128(w128):
    return " ".join("%02x" % int(w128[i]) for i in range(16))

def readDTH(infile, checkStart=False, checkEnd=False):
    global events
    dth128 = infile.read(16)
    if len(dth128) != 16: raise EOFError
    if not(dth128[0] == 0x47 and dth128[1] == 0x5a):
        raise RuntimeError("Error, expected DTH header, found %s" % (s128(dth128)))
    bstart, bend, bsize = getbit(dth128,47), getbit(dth128,46), int(dth128[48//8])
    if events < options.nprint: print("DTH header %s: start %d, end %d, length %d" % (s128(dth128), bstart, bend, bsize))
    if checkStart and not bstart:
        raise RuntimeError("Error: DTH header doesn't have the start of block bit active")
    if checkEnd and not bend:
        raise RuntimeError("Error: DTH header doesn't have the end of block bit active")
    return dth128, bstart, bend, bsize

def readSRHeader(infile, checkEvNo=False):
    global events
    sr128s = infile.read(16)    
    if len(sr128s) != 16: raise EOFError
    evno = int.from_bytes(sr128s[8:12], sys.byteorder)
    if events < options.nprint: print("SR header %s: event %u" % (s128(sr128s), evno))
    if sr128s[15] != 0x55:
        raise RuntimeError("Error, expected SR header, found %s" % (s128(sr128s)))
    if checkEvNo and evno != (events+1):
        raise RuntimeError("Error: mismatch event number from SR header (%u) vs count %u" % (evno, events+1))
    return sr128s, evno

def readSRTrailer(infile):
    global events
    sr128e = infile.read(16)    
    if len(sr128e) != 16: raise EOFError
    if sr128e[15] != 0xaa:
        raise RuntimeError("Error, expected SR trailer, found %s" % (s128(sr128s)))
    eorbit = int.from_bytes(sr128e[4:8], sys.byteorder)
    ebx    = int.from_bytes(sr128e[8:10], sys.byteorder) & 0xFFF;
    elen   = (int.from_bytes(sr128e[9:13], sys.byteorder) >> 4) & 0xFFFFF;
    if events < options.nprint: print("SR trailer %s: orbit %u (%x), bx %u, elen %d" % (s128(sr128e), eorbit, eorbit, ebx, elen)) 
    return sr128e, eorbit, ebx, elen

def readPuppiEventHeader(infile, bsize=-1):
    global events
    global puppis
    evhead = int.from_bytes(infile.read(8), sys.byteorder)
    bits = evhead >> 62
    if bits != 0b10:
        raise RuntimeError("Bad event header %016x, bits 63-62 are %1d%1d, expected 10" % (evhead,bits/2,bits%2))
    orbit = (evhead >> 24) & 0xFFFFFFFF
    bx = (evhead >> 12) & 0xFFF
    npuppi = evhead & 0xFFF;
    puppis += npuppi
    if options.npuppi:
        n64 = 1 + npuppi; n64 += n64 % 2 # round up to an event number
        if bsize > 0 and n64 != (bsize - 2) * 2:
            print("Event header %016x, orbit %u (%x), bx %u, npuppi %d, n64 %u" % (evhead, orbit, orbit, bx, npuppi, n64))
            raise RuntimeError("Mismatch between event size from DTH header %u and Event header %u" % ((bsize - 2) * 2, n64))
    else:
        n64 = (bsize - 2) * 2;
    if events < options.nprint: print("Event header %016x, orbit %u (%x), bx %u, npuppi %d, n64 %u" % (evhead, orbit, orbit, bx, npuppi, n64))
    return evhead, orbit, bx, npuppi, n64 

def checkSize(dthsize, srsize):
    #if srsize != dthsize:
    if srsize != dthsize:
        raise RuntimeError("Mismatch between size in DTH header (%d) and SR trailer (%d)" % (dthsize, srsize))

def checkOrbitAndBx(orbit, bx, SRorbit,SRbx):
    if (SRorbit,SRbx) != (orbit, bx):
        raise RuntimeError("Mismatch between Event header (orbit %u, bx %u) and SR trailer (orbit %u, bx %u)" % (orbit, bx, SRorbit,SRbx))

def countEventsAndOrbits(orbit,bx):
    global events, orbits, oldbx, oldorbit
    global options
    if orbit != oldorbit:
        if oldorbit != -1 and (orbit != oldorbit + 1):
            raise RuntimeError("Error, orbit %u found after %u" % (orbit, oldorbit))
        orbits += 1; oldorbit = orbit
        if oldbx != -1 and bx != options.firstbx :    
            raise RuntimeError("Error, orbit starts with bx %u instead of %u" % (bx, options.firstbx))
        oldbx = bx
    else:
        if oldbx != -1 and bx != oldbx + options.tmux:
            raise RuntimeError("Error, found %u instead of %u after bx %u" % (bx, oldbx + options.tmux, oldbx))
        oldbx = bx
    events += 1

infile = open(args[0], 'rb')
events, bxs, orbits, puppis = 0, 0, 0, 0
oldbx, oldorbit = -1, -1
nprint = 10
try:
    if options.orbitAggregation:
        while infile.readable():
            dth128, bstart, bend, bsize = readDTH(infile, checkStart=True)
            assert(bend or (bsize == 0xFF))
            chunks = [infile.read(16*bsize)]
            wholesize = bsize*2
            while bend != True:
                dth128, bstart, bend, bsize = readDTH(infile)
                assert(bstart == False)
                assert(bend or (bsize == 0xFF))
                chunks.append(infile.read(16*bsize))
                wholesize += bsize*2
            orbits += 1
            orbitbuff = io.BytesIO(bytes().join(chunks))
            sr128s, orbitno = readSRHeader(orbitbuff)
            #if orbitno != orbits:
            #    raise RuntimeError("Error: mismatch orbit number from SR header (%u) vs count %u" % (orbitno, orbits))
            remaining = wholesize - 2
            while remaining > 2:
               evhead, orbit, bx, npuppi, n64 = readPuppiEventHeader(orbitbuff) 
               countEventsAndOrbits(orbit,bx)
               toprint = orbitbuff.read(8*(n64-1))
               if events < options.nprint:
                  print(": %s" % ("  ".join(("%016x" % int.from_bytes(toprint[8*i:8*i+8], sys.byteorder)) for i in range(n64-1))))
               remaining -= n64
            sr128e, eorbit, ebx, elen = readSRTrailer(orbitbuff)
            checkSize(wholesize//2, elen)
            if events % nprint == 0:
                print("Read %10u events, %7u orbits" % (events, orbits))
                nprint *= 2
            if options.maxev > 0 and events > options.maxev:
                break
    else:
        while infile.readable():
            dth128, bstart, bend, bsize = readDTH(infile, checkStart=True, checkEnd=True)
            sr128s, evno = readSRHeader(infile, checkEvNo=True)
            evhead, orbit, bx, npuppi, n64 = readPuppiEventHeader(infile, bsize)
            infile.read(8*(n64-1))
            sr128e, eorbit, ebx, elen = readSRTrailer(infile)
            checkSize(bsize, elen)
            checkOrbitAndBx(orbit, bx, eorbit, ebx)
            countEventsAndOrbits(orbit, bx)
            if events % nprint == 0:
                print("Read %10u events, %7u orbits" % (events, orbits))
                nprint *= 2
            if options.maxev > 0 and events > options.maxev:
                break
except EOFError:
    print("Got end of file")
print("End after reading %10u events, %7u orbits, %u candidates" % (events, orbits, puppis))
