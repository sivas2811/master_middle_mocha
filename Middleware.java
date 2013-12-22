import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
 
public class Middleware {

    private String masterIP;
    
    public Middleware(){
	this.masterIP = "23.22.232.19"; 
    }
    
    public String getMasterIP(){
    	return this.masterIP;
    }
    public static void main(String[] args) throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool
                        (10, new ThreadFactory(){
                                public Thread newThread(Runnable runnable){
                                        Thread thread = new MyThreadFactory().newThread(runnable);
                                        return thread;
                                }
                        });
      Middleware mw = new Middleware();
     
        int ii =0;
        // Listening to data from clients at 9090
        ServerSocket Listener = new ServerSocket(9090);
        Socket socket ;
        while(true){
	//System.out.println("Listening at 9090");
        socket = Listener.accept();
	ii = (ii+1)%2;
	System.out.println("Some input is accepted" + ii);
                 BufferedReader in = 
                                         new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 String input = in.readLine();
             if(input.equalsIgnoreCase("stop")){
            	 Socket s = new Socket(mw.getMasterIP(),9898);
                 PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                 out.println(input);
                 s.close();
                 break;
             	}
            //System.out.println("Input from client:"+ input);
            Runnable worker = new WorkerThread_Middleware(input,mw,socket);
            executor.execute(worker);  
            
        }
        socket.close();
        Listener.close();
    }
 
}
/* TODO
 *  *  * 1. SEnd request to appropriate data node
 *   *   * 2. Store client id and return with the reply message
 *    *    * 
 *     *     */
