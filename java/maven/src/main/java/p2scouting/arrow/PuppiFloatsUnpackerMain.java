package p2scouting.arrow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import p2scouting.core.PuppiSOAFloatRecord;
import p2scouting.core.PuppiUnpacker;

/**
 * Simple class that reads a binary (native64) file and uses a PuppiFloatsWriter
 * to convert it to arrow IPC
 */

public class PuppiFloatsUnpackerMain {
    public static void main(String[] args) {
        BufferAllocator allocator = new RootAllocator();
        PuppiFloatsWriter writer = args.length >= 2 ? new PuppiFloatsWriter(allocator, 3564) : null;
        long orbits = 0, lastorbit = 0, events = 0, candidates = 0, bytes = 0;
        try {
            if (writer != null)
                writer.startFile(args[1]);
            var path = Path.of(args[0]);
            System.out.println("Opening " + path + ": exists? " + path.toFile().exists());
            var fc = FileChannel.open(path, StandardOpenOption.READ);
            var hbuff = ByteBuffer.allocate(8); // LongBuffer.wrap(header);
            var buff = ByteBuffer.allocate(8 * 256); // LongBuffer.wrap(data);
            System.out.println("Our byte order is "
                    + (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN ? "big" : "little") + "-endian.\n");
            hbuff.order(ByteOrder.nativeOrder());
            buff.order(ByteOrder.nativeOrder());
            var t0 = System.nanoTime();
            while (fc.isOpen()) {
                int nb = fc.read(hbuff); // read header
                if (nb == -1)
                    break;
                if (nb != 8)
                    throw new IOException("Got only " + nb + " bytes for the header");
                // now convert to long
                hbuff.flip();
                long h = hbuff.getLong();
                hbuff.clear();
                if (h == 0)
                    continue;
                int npuppi = (int) (h & 0xFF);
                short bx = (short) ((h >> 12) & 0xFFF);
                short run = (short) ((h >> 54) & 0x3F);
                int orbit = (int) ((h >>> 24) & 0xFFFF_FFFF);
                boolean good = (h & 0x2000_0000_0000_0000l) == 0;
                if (orbit != lastorbit) {
                    orbits++;
                    lastorbit = orbit;
                }
                events++;
                candidates += npuppi;
                bytes += (npuppi + 1) * 8;
                PuppiSOAFloatRecord puppis = null;
                if (npuppi > 0) {
                    buff.limit(8 * npuppi);
                    nb = fc.read(buff);
                    if (nb != 8 * npuppi)
                        throw new IOException("Got only " + nb + " bytes for the payload instead of " + (8 * npuppi));
                    buff.flip();
                    puppis = PuppiUnpacker.unpackMany(buff.asLongBuffer());
                }
                if (writer != null)
                    writer.fillEvent(run, orbit, bx, good, puppis);
                buff.clear();
            }
            if (writer != null)
                writer.doneFile();
            var t1 = System.nanoTime();
            var time = (1e-9 * (t1 - t0));
            double inrate = bytes / (1024. * 1024.) / time;
            System.out.printf(
                    "Done in %.2fs. Processed %d orbits, %d events, %d candidates, Event rate: %.1f kHz (40 MHz / %.1f), input data rate %.1f MB/s (%.1f Gbps)\n",
                    time,
                    orbits, events, candidates,
                    events / time / 1000.,
                    (40e6 * time / events),
                    inrate,
                    inrate * 8 / 1024.);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
