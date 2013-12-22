import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
 
public class WorkerThread_Master implements Runnable {
     
    private String input;
    private Master master;
    private Socket socket;
 
    public WorkerThread_Master(String input,Master master, Socket socket){
        this.input=input;
        this.master = master;
	this.socket = socket;
    }
    @Override
    public void run() {
      //  //System.out.println(Thread.currentThread().getName()+" Start. Command = "+input);
        try {
                        processCommand(Thread.currentThread().getName());
                } catch (IOException e) {
                        e.printStackTrace();
                }
    }
 
    private void processCommand(String port) throws UnknownHostException, IOException {
    	String[] splits = input.split(":");
		 Long key = Long.parseLong(splits[0]);
		 //System.out.println("key:"+ key);
		 //Get ServerID for the key
		 String serverID = master.get(key);
		 //System.out.println("serverID:"+serverID);
		 
		 //Reply back to the worker thread on the middleware
		 //String serverAddress = master.middleWareIP();
		 //Socket s = new Socket(serverAddress, Integer.parseInt(splits[1]));
		 PrintWriter out = new PrintWriter(this.socket.getOutputStream(), true);
		 out.println(serverID);
                 
         }
         
    @Override
    public String toString(){
        return this.input;
    }
}
