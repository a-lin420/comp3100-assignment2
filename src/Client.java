import java.net.*;
import java.util.*;
import java.io.*;

public class Client {

    // s
    private static Socket s;

    // s args
    private static final String hostname = "localhost";
    private static final int serverPort = 50000;

    // streams
    private static BufferedReader bfr;
    private static InputStreamReader din;
    private static DataOutputStream dout;

    // commands
    private static final String HELO = "HELO";
    private static final String OK = "OK";
    private static final String AUTH = "AUTH";
    private static final String REDY = "REDY";
    private static final String JOBN = "JOBN";
    private static final String JCPL = "JCPL";
    private static final String GETS = "GETS";
    private static final String SCHD = "SCHD";
    private static final String LSTJ = "LSTJ";
    private static final String NONE = "NONE";
    private static final String QUIT = "QUIT";

    // buffer fields
    private static String stringBuffer; /* will hold the current message from the server stored in a string
                                                                       (created from charArray)        */
    private static String[] fieldBuffer; /* will hold the current message from the server as an array of strings
                                                                       (created from stringBuffer)     */

    private static String scheduleString; // string to be scheduled


    public static void main(String[] args) throws IOException {
        setup();

        try {
            writeBytes(HELO); // client sends HELO
            stringBuffer = bfr.readLine();

            // server replies with OK

            // System.out.println("sent AUTH username");
            writeBytes(AUTH + " " + System.getProperty("user.name"));

            // server replies with OK after printing out a welcome message and writing system info
            stringBuffer = bfr.readLine();

            //System.out.println("Sending REDY ...");
            writeBytes(REDY);
            //System.out.println("REDY sent.");
            

            while (!(stringBuffer = bfr.readLine()).contains(NONE)) {

                if (stringBuffer.contains(JOBN)) {
                    // STORE JOB DATA
                    //System.out.println(stringBuffer); // print JOB info
                    fieldBuffer = stringBuffer.split(" "); /* split String into array of strings
                                                              (each string being a field of JOBN) */

                    Job job = new Job(fieldBuffer); // create new Job object with data from fieldBuffer


                    // get list of capable servers (state information)
                    writeBytes(GETS + " Capable " + job.getCore() + " " + job.getMemory() + " " + job.getDisk());

                    // DATA _ _ message
                    stringBuffer = bfr.readLine();
                    // System.out.println("DATA received : " + stringBuffer);
                    fieldBuffer = stringBuffer.split(" "); 

                    int numCapableServer = Integer.parseInt(fieldBuffer[1]); // fieldBuffer[1] -> no. of capable servers

                    writeBytes(OK); // confirmation for receiving DATA

                    // READ & STORE LIST OF CAPABLE SERVERS TO HANDLE CURRENT JOB
                    ArrayList<Server> capableServersList = getCapableServerList(numCapableServer);
                    
                    writeBytes(OK); // confirmation for receiving server list
                    stringBuffer = bfr.readLine(); // server replies .


                    // ALGORITHM FOR JOB SCHEDULING
                    // determines which server each job is sent/scheduled to
                    Server optimalServer = getOptimalServer(capableServersList, job);


                    /* SCHEDULE JOB */
                    scheduleString = SCHD + " " + job.id + " " + optimalServer.type + " " + optimalServer.id;
                    writeBytes(scheduleString);

                    // System.out.println("---------------"); 

                    // request new job
                    writeBytes(REDY); // send REDY for the next job
                } 
                else if (stringBuffer.contains(JCPL)) {
                    writeBytes(REDY);
                }
            }

            //System.out.println("TERMINATING CONNECTION ...");
            
            writeBytes(QUIT);

            //System.out.println("CONNECTION TERMINATED.");

            close();
        } catch (UnknownHostException e) {
            System.out.println("Unknown Host Exception: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("End of File Exception: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO Exception: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
        }
    }

    
    public static ArrayList<Server> getCapableServerList(int listSize) throws IOException {
        ArrayList<Server> capableServersList = new ArrayList<>();
        
        // System.out.println("* * List of capable servers * *");
        for (int i = 0; i < listSize; i++) {
            stringBuffer = bfr.readLine(); // read single server information

            // System.out.println(stringBuffer); // print capable SERVER info
            fieldBuffer = stringBuffer.split(" ");

            Server capable = new Server(fieldBuffer);
            capableServersList.add(capable);
        }
        return capableServersList;
    }

    public static Server getOptimalServer(ArrayList<Server> capableServersList, Job job) throws IOException {
        ArrayList<Server> activeNoWaitingJobs = new ArrayList<>();
        ArrayList<Server> activeServers =  new ArrayList<>(); 
        ArrayList<Server> inactiveServers = new ArrayList<>();

        for (Server currServer : capableServersList) {
            if (currServer.isIdle()) {
                // Idle ... Please send jobs.
                return currServer;
            }
            else if (currServer.isActive()) {
                if (currServer.hasNoWaitingJobs()) {
                    // Running job(s) | No jobs in queue
                    activeNoWaitingJobs.add(currServer);
                } else {
                    // Running job(s) | Jobs in queue
                    activeServers.add(currServer);
                }
            } 
            else if (currServer.isInactive()) {
                // Inactive. Boot me, please.
                inactiveServers.add(currServer);
            }  
        }
        // Loop exits when there are no idle servers on the list
        // optimalServer remains as the smallest capable server


        // ORDER OF PRIORITY (if no idle servers) 
        //    active (no waiting jobs) > inactive > active (with least waiting jobs) > booting (with least waiting jobs)
        if (!activeNoWaitingJobs.isEmpty()) {
            return findServerWithLeastWaitingTime(activeNoWaitingJobs, job);
        } 
        else if (!inactiveServers.isEmpty()) {
            return inactiveServers.get(0); // boot smallest
        } 
        else if (!activeServers.isEmpty()) {
            return findLeastWaitingJobs(activeServers); // get largest possible server 
        }
        // if ALL capable servers are BOOTING, return Server with LEAST number of WAITING jobs
        return findLeastWaitingJobs(capableServersList);           
    }


    // findServerWithLeastWaitingTime | return the server with the 
    public static Server findServerWithLeastWaitingTime(ArrayList<Server> activeNoWaitingJobs, Job job) throws IOException {
        int leastWaitingIndex = 0;

        ArrayList<Integer> latestCompletionTimes = new ArrayList<>(); // store latest est. completion time 
                                                                      // relative to active server indices
        for (Server active : activeNoWaitingJobs) {
            // Get list of RUNNING JOBS on current ACTIVE Server
            ArrayList<String> runningJobs = getRunningJobsList(active);

            // Find JOB with LATEST completion time
            int latestFinishTime = findLatestCompletionTime(runningJobs);
            latestCompletionTimes.add(latestFinishTime);
        }

        int leastWaitingTime = latestCompletionTimes.get(leastWaitingIndex) - job.submitTime; // set to first item on list
        // Find index of server with lowest est. waiting time 
        for (Integer estCompTime : latestCompletionTimes) { 
            int currJobSubmitTime = job.submitTime;

            if (currJobSubmitTime < estCompTime) {
                // Get difference between the current job's submit time and the est. completion time of jobs on a server
                int estWaitingTime = estCompTime - currJobSubmitTime; 
                if (estWaitingTime < leastWaitingTime) {
                    leastWaitingIndex = latestCompletionTimes.indexOf(estCompTime);
                    leastWaitingTime = estWaitingTime;
                }
            } else {
                // if the current job to be scheduled is submitted after the latest running job completes
                //                                                             schedule it to that server
                leastWaitingIndex = latestCompletionTimes.indexOf(estCompTime);
                break;
            }
        }
        return activeNoWaitingJobs.get(leastWaitingIndex);
    }


    // getRunningJobsList | returns a list of all running jobs on a particular Server
    public static ArrayList<String> getRunningJobsList(Server active) throws IOException {
        // Request all running jobs info from server
        writeBytes(LSTJ + " " + active.type + " " + active.id);

        stringBuffer = bfr.readLine(); // DATA _ _ from server

        writeBytes(OK); // notify ready to receive rJob info

        // Get LIST of RUNNING JOBS on current Server
        ArrayList<String> runningJobs = new ArrayList<>();

        // System.out.println("* * RUNNING JOBS * *");
        while (!(stringBuffer = bfr.readLine()).equals(".")) {
            // STRING BUFFER = running job info
            runningJobs.add(stringBuffer); // add job to list
            writeBytes(OK);
        }
        return runningJobs;
    }


    // findLatestCompletionTime | returns the latest possible completion time of running jobs in a Server
    public static Integer findLatestCompletionTime(ArrayList<String> runningJobs) {
        int latestFinishTime = 0;

        for (String rJob : runningJobs) {
            fieldBuffer = rJob.split(" ");

            int startTime = Integer.parseInt(fieldBuffer[2]);   // job start time
            int estRunTime = Integer.parseInt(fieldBuffer[3]);  // job estimated runtime
            int estFinishTime = startTime + estRunTime;

            // stable sort
            if (estFinishTime > latestFinishTime) {
                latestFinishTime = estFinishTime;
            }
        }
        return latestFinishTime;
    }


    // findLeastWaitingJobs | returns the server with the least number of waiting jobs
    public static Server findLeastWaitingJobs(ArrayList<Server> activeServers) {
        Server optimal = activeServers.get(0);
        int minWaitingJobs = optimal.getNumWaitingJobs();

        for (Server s : activeServers) {
            // stable sort - optimal is always set to the smallest possible server
            if (s.getNumWaitingJobs() < minWaitingJobs) {
                optimal = s;
                minWaitingJobs = s.getNumWaitingJobs();
            }
        }
        return optimal;
    }


    public static void setup() throws IOException {
        s = new Socket(hostname, serverPort); // socket with host IP of 127.0.0.1 (localhost), server port of 50000

        din = new InputStreamReader(s.getInputStream());
        bfr = new BufferedReader(din);
        dout = new DataOutputStream(s.getOutputStream());
    }


    public static void writeBytes(String message) throws IOException {
        dout.write((message + "\n").getBytes());
        dout.flush();
    }
    
    
    public static void close() throws IOException {
        bfr.close();
        din.close();
        dout.close();
        s.close();
    }

}