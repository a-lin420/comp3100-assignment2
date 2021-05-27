import java.net.*;
import java.util.*;
import java.io.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

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
    private static final String EJWT = "EJWT";
    private static final String NONE = "NONE";
    private static final String QUIT = "QUIT";

    // buffer fields
    private static String stringBuffer; /* will hold the current message from the server stored in a string
                                                                       (created from charArray)        */
    private static String[] fieldBuffer; /* will hold the current message from the server as an array of strings
                                                                       (created from stringBuffer)     */

    private static String scheduleString; // string to be scheduled

    // create server/list objects
    private static Server largestServer;

    public static void main(String[] args) throws IOException {
        setup();

        try {
            writeBytes(HELO); // client sends HELO

            // server replies with OK

            System.out.println("sent AUTH username");
            writeBytes(AUTH + " " + System.getProperty("user.name"));

            // server replies with OK after printing out a welcome message and writing system info

            // setLargestServer();

            System.out.println("Sending REDY ...");
            writeBytes(REDY);
            System.out.println("REDY sent.");
            
            while (!(stringBuffer = bfr.readLine()).contains(NONE)) {

                if (stringBuffer.contains(JOBN)) {
                    System.out.println("---------------");

                    System.out.println(stringBuffer); // print JOB info
                    fieldBuffer = stringBuffer.split(" "); /* split String into array of strings
                                                              (each string being a field of JOBN) */

                    Job job = new Job(fieldBuffer); // create new Job object with data from fieldBuffer

                    // get list of capable servers (state information)
                    writeBytes(GETS + " Capable " + job.core + " " + job.memory + " " + job.disk);
                    writeBytes(OK);



                    // DATA _ _ message
                    stringBuffer = bfr.readLine();
                    System.out.println("DATA received : " + stringBuffer);

                    fieldBuffer = stringBuffer.split(" "); 
                    int numCapableServer = Integer.parseInt(fieldBuffer[1]); // fieldBuffer[1] -> no. of capable servers

                    // send list of capable server (one at a time)
                    writeBytes(OK); 

                    ArrayList<Server> capableServersList = new ArrayList<>();
                    // System.out.println("* * List of capable servers * *");
                    for (int i = 0; i < numCapableServer; i++) {
                        stringBuffer = bfr.readLine(); // read single server information

                        // System.out.println(stringBuffer); // print capable SERVER info
                        fieldBuffer = stringBuffer.split(" ");

                        Server capable = new Server(fieldBuffer);
                        capableServersList.add(capable);
                    }

                    // QUERY SERVER EST. WAITING TIME
                    // for (Server svr : capableServersList) {
                    //     writeBytes(EJWT + " " + svr.type + " " + svr.id);
                        
                    //     readStringBuffer();
                    //     System.out.println("Est. waiting time : " + svr.type + " " + svr.id + " | " + stringBuffer);
                    // }



                    // ALGORITHM FOR JOB SCHEDULING
                    // determines which server each job is sent/scheduled to
                    largestServer = capableServersList.get(capableServersList.size() - 1);
                    System.out.println("Chosen server | " + largestServer.type + " " + largestServer.id);

                    System.out.println("---------------");



                    /* SCHEDULE JOB */
                    scheduleString = SCHD + " " + job.id + " " + largestServer.type + " " + largestServer.id;
                    writeBytes(scheduleString);



                    // request new job
                    writeBytes(REDY); // send REDY for the next job
                } 
                else if (stringBuffer.contains(JCPL)) {
                    writeBytes(REDY); // send REDY for the next job
                } 
            }

            System.out.println("TERMINATING CONNECTION ...");
            
            writeBytes(QUIT);

            System.out.println("CONNECTION TERMINATED.");

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