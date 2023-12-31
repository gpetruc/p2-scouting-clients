package p2scouting.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class ReadFile {
    public static void usage(String err) {
        System.out.println("Usage: ReadFile [-u] file. " + err);
        System.exit(1);
    }

    public static void main(String[] args) {
        long orbits = 0, lastorbit = 0, events = 0, candidates = 0, bytes = 0;
        double sumpt = 0;
        var t0 = System.nanoTime();
        boolean unpack = false;
        try {
            Path path = null;
            for (int i = 0; i < args.length; ++i) {
                if (args[i].equals("-u") || args[i].equals("--unpack")) {
                    unpack = true;
                } else {
                    if (path != null)
                        usage("Duplicate argument '" + args[i] + "'");
                    path = Path.of(args[i]);
                }
            }
            if (path == null) {
                usage("Missing argument");
                return;
            }
            System.out.println("Opening " + path + ": exists? " + path.toFile().exists());
            var fc = FileChannel.open(path, StandardOpenOption.READ);
            var hbuff = ByteBuffer.allocate(8); // LongBuffer.wrap(header);
            var buff = ByteBuffer.allocate(8 * 256); // LongBuffer.wrap(data);
            System.out.println("Our byte order is "
                    + (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN ? "big" : "little") + "-endian.\n");
            hbuff.order(ByteOrder.LITTLE_ENDIAN);
            buff.order(ByteOrder.LITTLE_ENDIAN);
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
                ScoutingEventHeaderRecord eh = ScoutingEventHeaderRecord.decode(h);
                int npuppi = eh.nwords();
                if (eh.orbit() != lastorbit) {
                    orbits++;
                    lastorbit = eh.orbit();
                }
                events++;
                candidates += npuppi;
                bytes += (npuppi + 1) * 8;
                if (npuppi > 0) {
                    buff.limit(8 * npuppi);
                    nb = fc.read(buff);
                    if (nb != 8 * npuppi)
                        throw new IOException("Got only " + nb + " bytes for the payload instead of " + (8 * npuppi));
                    buff.flip();
                    if (unpack) {
                        var puppis = PuppiUnpacker.unpackMany(buff.asLongBuffer());
                        for (float p : puppis.pt())
                            sumpt += p;
                    }
                    buff.clear();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        if (unpack)
            System.out.printf("Sumpt %.1f\n", sumpt);
    }
}