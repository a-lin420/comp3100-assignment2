public class Server {
    protected String type;
    protected int id;
    protected String state;
    protected int startTime;
    // protected int bootUpTime;
    // protected float hourlyRate;
    protected int core;
    protected int memory;
    protected int disk;
    protected int numWaitingJobs;
    protected int numRunningJobs;

    public Server(String[] fieldBuffer) {
        this.type = fieldBuffer[0];
        this.id = Integer.parseInt(fieldBuffer[1]);
        this.state = fieldBuffer[2];
        this.startTime = Integer.parseInt(fieldBuffer[3]);
        this.core = Integer.parseInt(fieldBuffer[4]);
        this.memory = Integer.parseInt(fieldBuffer[5]);
        this.disk = Integer.parseInt(fieldBuffer[6]);
        this.numWaitingJobs = Integer.parseInt(fieldBuffer[7]);
        this.numRunningJobs = Integer.parseInt(fieldBuffer[8]);
    }

}