public class Server {
    protected int id;
    protected String type;
    protected int limit;
    protected int bootUpTime;
    protected float hourlyRate;
    protected int core;
    protected int memory;
    protected int disk;

    public Server(int id, String t, int l, int b, float hr, int c, int m, int d) {
        this.id = id;
        this.type = t;
        this.limit = l;
        this.bootUpTime = b;
        this.hourlyRate = hr;
        this.core = c;
        this.memory = m;
        this.disk = d;

    }

}