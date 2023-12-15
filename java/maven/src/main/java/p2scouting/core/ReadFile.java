package p2scouting.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class ReadFile {
    public static void main(String[] args) {
        long orbits = 0, lastorbit = 0, events = 0, candidates = 0, bytes = 0;
        double sumpt = 0;
        try {
            var path = Path.of(args[0]);
            System.out.println("Opening " + path + ": exists? " + path.toFile().exists());
            var fc = FileChannel.open(path, StandardOpenOption.READ);
            var hbuff = ByteBuffer.allocate(8); // LongBuffer.wrap(header);
            var buff = ByteBuffer.allocate(8 * 256); // LongBuffer.wrap(data);
            System.out.println("Our byte order is "
                    + (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN ? "big" : "little") + "-endian.\n");
            hbuff.order(ByteOrder.nativeOrder());
            buff.order(ByteOrder.nativeOrder());
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
                int npuppi = (int)(h & 0xFF);
                long orbit = ((h>>>24) & 0xFFFF_FFFF);
                if (orbit != lastorbit) {
                    orbits++;
                    lastorbit = orbit;
                }
                events++;
                candidates += npuppi;
                bytes += (npuppi+1)*8;
                if (npuppi > 0) {
                    buff.limit(8 * npuppi);
                    nb = fc.read(buff);
                    if (nb != 8 * npuppi)
                        throw new IOException("Got only " + nb + " bytes for the payload instead of " + (8 * npuppi));
                    buff.flip();
                    var puppis = PuppiUnpacker.unpackMany(buff.asLongBuffer());
                    for (float p : puppis.pt()) sumpt += p;
                    buff.clear();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.printf("Read %d orbits, %d events, %d candidates, %d bytes, sumpt %.1f\n", orbits, events, candidates, bytes, sumpt);
    }
}