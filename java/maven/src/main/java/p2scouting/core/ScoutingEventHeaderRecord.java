package p2scouting.core;

public record ScoutingEventHeaderRecord(short run, int orbit, short bx, boolean good, short nwords) {
    public final static int BITS_NWORDS = 12;
    public final static int BITS_BX = 12;
    public final static int BITS_ORBIT = 32;
    public final static int BITS_RUN = 5;
    public final static int BITS_ERR = 1;
    public final static int BITS_TYPE = 2;
    public final static int OFFS_NWORDS = 0;
    public final static int OFFS_BX = OFFS_NWORDS + BITS_NWORDS;
    public final static int OFFS_ORBIT = OFFS_BX + BITS_BX;
    public final static int OFFS_RUN = OFFS_ORBIT + BITS_ORBIT;
    public final static int OFFS_ERR = OFFS_RUN + BITS_RUN;
    public final static int OFFS_TYPE = OFFS_ERR + BITS_ERR;
    public final static long TYPE_EH = 0b01;
    public final static int MASK_NWORDS = (1 << BITS_NWORDS) - 1;
    public final static int MASK_BX = (1 << BITS_BX) - 1;
    public final static int MASK_ORBIT = (int)((1l << BITS_ORBIT) - 1);
    public final static int MASK_RUN = (1 << BITS_RUN) - 1;
    public final static long FULL_MASK_ERR = 1 << OFFS_ERR;

    public final long encode() {
        return (TYPE_EH << OFFS_TYPE) |
         ((good ? 0l : 1l) << OFFS_ERR) |
         (((long)run) << OFFS_RUN) |
         (((long)orbit) << OFFS_ORBIT) |
         (((long)bx) << OFFS_BX) |
         (((long)nwords) << OFFS_NWORDS);
    }

    public static ScoutingEventHeaderRecord decode(long header) {
        short run = (short) ((header >>> OFFS_RUN) & MASK_RUN);
        int orbit = (int) ((header >>> OFFS_ORBIT) & MASK_ORBIT);
        short bx = (short) ((header >>> OFFS_BX) & MASK_BX);
        boolean good = (header & FULL_MASK_ERR) == 0l;
        short nwords = (short)(header & MASK_NWORDS);;
        return new ScoutingEventHeaderRecord(run, orbit, bx, good, nwords);
    }
}
