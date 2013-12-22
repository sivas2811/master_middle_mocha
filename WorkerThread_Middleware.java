import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
 
public class WorkerThread_Middleware implements Runnable {
     
    private String input;
    private Middleware mw;
    private Socket socket;
     
    public WorkerThread_Middleware(String input,Middleware mw,Socket socket){
        this.input=input;
        this.mw = mw;
        this.socket = socket;
    }
    /** Helper function for calculating hash
     *  
     * @param data byte array to hash
     * @param length length of the array to hash
     * @return 64 bit hash of the given string
     */
    public static long hash64( final byte[] data, int length, int seed) {
                final long m = 0xc6a4a7935bd1e995L;
                final int r = 47;
                long h = (seed&0xffffffffl)^(length*m);
                int length8 = length/8;
                for (int i=0; i<length8; i++) {
                        final int i8 = i*8;
                        long k =  ((long)data[i8+0]&0xff)      +(((long)data[i8+1]&0xff)<<8)
                                        +(((long)data[i8+2]&0xff)<<16) +(((long)data[i8+3]&0xff)<<24)
                                        +(((long)data[i8+4]&0xff)<<32) +(((long)data[i8+5]&0xff)<<40)
                                        +(((long)data[i8+6]&0xff)<<48) +(((long)data[i8+7]&0xff)<<56);
   
                        k *= m;
                        k ^= k >>> r;
                        k *= m;
   
                        h ^= k;
                        h *= m; 
                }
   
                switch (length%8) {
                case 7: h ^= (long)(data[(length&~7)+6]&0xff) << 48;
                case 6: h ^= (long)(data[(length&~7)+5]&0xff) << 40;
                case 5: h ^= (long)(data[(length&~7)+4]&0xff) << 32;
                case 4: h ^= (long)(data[(length&~7)+3]&0xff) << 24;
                case 3: h ^= (long)(data[(length&~7)+2]&0xff) << 16;
                case 2: h ^= (long)(data[(length&~7)+1]&0xff) << 8;
                case 1: h ^= (long)(data[length&~7]&0xff);
                        h *= m;
                };
         
                h ^= h >>> r;
                h *= m;
                h ^= h >>> r;
                return h;
        }
   
        /** Generates 64 bit hash from byte array with default seed value.
         * 
         * @param data byte array to hash
         * @param length length of the array to hash
         * @return 64 bit hash of the given string
         */
        public static long hash64( final byte[] data, int length) {
                return hash64( data, length, 0xe17a1465);
        }
        /** Generates 64 bit hash from a string.
         * 
         * @param text string to hash
         * @return 64 bit hash of the given string
         */
        public static long hash64( final String text) {
                final byte[] bytes = text.getBytes(); 
                return hash64( bytes, bytes.length);
        }
  // s;tc;topic;content (success), s;up;user;password(success), s;ut;user;topic (success)
  // r;up;user;password(authenticated), r;tc;topic(content), r;ut;user(topic)
        
    @Override
    public void run() {
      //  //System.out.println(Thread.currentThread().getName()+" Start. Command = "+input);
        try {
                        processCommand(Thread.currentThread().getName());
                } catch (IOException e) {
                        e.printStackTrace();
                } catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    }
 
    private void processCommand(String port) throws UnknownHostException, IOException, ClassNotFoundException {
    	
    	//Parsing the input to hash the key
         String[] splits = this.input.split(";");
         String key = splits[2];
         long hashval = hash64(key);
         
         if(!(splits[0].equalsIgnoreCase("r")&& splits[1].equalsIgnoreCase("tc"))){
		 // Sending the hash value to the master
		 String serverAddress = mw.getMasterIP();
		 //System.out.println("Trying to send the input "+input+ "to Master" +serverAddress);
		 Socket s = new Socket(serverAddress,9898);
		 PrintWriter out = new PrintWriter(s.getOutputStream(), true);
		 out.println(hashval+":"+port);
	 
		 //Getting datanode address from Master
		 //ServerSocket Listener = new ServerSocket(Integer.parseInt(port));
		 //Socket socket = Listener.accept();
		 BufferedReader in = new BufferedReader
				 (new InputStreamReader(s.getInputStream()));
		 String  datanodeAddress = in.readLine();
		 //System.out.println("Returned value for:" + key+" is" + datanodeAddress );
		 out.close();
		 in.close();
		 s.close();
			 
		 //Sending to datanode
		 Socket datanode_socket = new Socket(datanodeAddress, 8000);
		 out = new PrintWriter(datanode_socket.getOutputStream(), true);
		 out.println(this.input+";"+port);
		 
		 //Receiving from datanode
		 if(input.startsWith("s")){
			 //socket = Listener.accept();
			 BufferedReader inp =  new BufferedReader(new InputStreamReader(datanode_socket.getInputStream()));
			 String output = inp.readLine();
			 //System.out.println("Output for command " +this.input +" is" + output);
			 out.close();
			 inp.close(); 
			  
			 //Send to WebSErver
			// datanode_socket = new Socket("54.205.185.249", 9090);
			 out = new PrintWriter(this.socket.getOutputStream(), true);
			 out.println(output);  
			 out.close();
		 }
		 else{
			 //socket = Listener.accept();
			 InputStream is = datanode_socket.getInputStream();
			 ObjectInputStream ois = new ObjectInputStream(is);
			 
			 ArrayList<String> result;
			 try
			 {
				result = (ArrayList<String>) ois.readObject();
				ois.close();
			 }
			 catch (Exception ex)
			 {
				result = new ArrayList<String>();
				//System.out.println("No data returned");
			 } 
			 String appendedString="";
			 for(int i=0;i<result.size();i++){
				 appendedString += result.get(i)+";";
			 }
			 
			 //Send to WebServer
			 //datanode_socket = new Socket("54.205.185.249",9090);
			 out = new PrintWriter(this.socket.getOutputStream(), true);
		         out.println(appendedString);  
			 out.close();
		 }
		 datanode_socket.close();                
		 socket.close();
		 //Listener.close();    
         } else{
        	 // Sending the hash values (topic)to the master
	         String serverAddress = mw.getMasterIP();
                 //System.out.println("Trying to send the input "+input+ "to Master" +serverAddress);
                 Socket s = new Socket(serverAddress,9898);
                 PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                 out.println(hashval+":"+port);      
             
                 //Getting datanode address from Master
                 //ServerSocket Listener = new ServerSocket(Integer.parseInt(port));
                 //Socket socket = Listener.accept();
                 BufferedReader in = new BufferedReader
                		 (new InputStreamReader(s.getInputStream()));
                 String  datanodeAddress = in.readLine();
                 //System.out.println("Returned value for:" + key+" is" + datanodeAddress );
	         out.close();
		 in.close();
                 s.close();
             
                 // Sending the hash values(user) to the master
                 serverAddress = mw.getMasterIP();
                 //System.out.println("Trying to send the input "+input+ "to Master" +serverAddress);
                 s = new Socket(serverAddress,9898);
                 out = new PrintWriter(s.getOutputStream(), true);
                 out.println(this.hash64(splits[3])+":"+ port); 

                 //Getting datanode address from Master
                 //socket = Listener.accept();
                 in = new BufferedReader
                		 (new InputStreamReader(s.getInputStream()));
                 String  datanodeAddress_user = in.readLine();
                 //System.out.println("Returned value for:" + key+" is" + datanodeAddress_user );
                 out.close();
		 in.close();
                 s.close();
                                       
                 //Sending to datanode
                 Socket datanode_socket = new Socket(datanodeAddress, 8000);
                 out = new PrintWriter(datanode_socket.getOutputStream(), true);
                 out.println(this.input+";"+port);
                 //System.out.println("Sending to data node");
                
                 //Listening from datanode
            	 //socket = Listener.accept();
            	 InputStream is = datanode_socket.getInputStream();
            	 ObjectInputStream ois = new ObjectInputStream(is);
            	 ArrayList<String> result = (ArrayList<String>) ois.readObject();
             	 //System.out.println("Receiving from data node");
	         out.close();             
            	 datanode_socket.close();
            	 //Listener.close();
		 ois.close();
		 
                 //Sending to datanode_user
                 datanode_socket = new Socket(datanodeAddress, 8000);
                 out = new PrintWriter(datanode_socket.getOutputStream(), true);
                 int port_datanode = Integer.parseInt(port)+15;
                 out.println("r;f;"+splits[3]+";"+Integer.toString(port_datanode));
                 //Listening from datanode_user
            	 //ServerSocket Listener1 = new ServerSocket(port_datanode);
		 ////System.out.println("Listening on port"+port_datanode);
                 //Socket socket1 = Listener1.accept();
                 InputStream is1 = datanode_socket.getInputStream();
            	 ObjectInputStream ois1 = new ObjectInputStream(is1);
            	 ArrayList<String> result_user = (ArrayList<String>) ois1.readObject();
            	 datanode_socket.close();
            	 //Listener1.close();
                 out.close();
		 ois1.close();
            	 //Join
            	 HashSet<String> following = new HashSet<String>();
            	 for(int i=0;i<result_user.size();i++){
            		following.add(result_user.get(i));
              	 }
            	 ArrayList<String> important = new ArrayList<String>();
            	 ArrayList<String> not_imp = new ArrayList<String>();
            	 for(int i=0;i<result.size();i++){
            		String[] split_tc = result.get(i).split("\\s+");
            		//System.out.println(split_tc[0]);
            		if(following.contains(split_tc[0])){
            			//System.out.println(split_tc[0]);
            			important.add(result.get(i));
            		}
            		else
            			not_imp.add(result.get(i));
            		
            	}
            	 
            	String appendedString="";
            	for(int i=0;i<important.size();i++){
            		 appendedString += important.get(i)+";";
            	}
            	 
            	for(int i=0;i<not_imp.size();i++){
            		 appendedString += not_imp.get(i)+";";
            	}
            	//System.out.println("The final result is" + appendedString); 
            	//Send to WebServer
            	//datanode_socket = new Socket("54.205.185.249",9090);
            	out = new PrintWriter(this.socket.getOutputStream(), true);
                out.println(appendedString);  
		out.close();
                //System.out.println("Sending to user program");
                socket.close();
                //Listener.close();  
         }        
                       
    }
         
    @Override
    public String toString(){
        return this.input;
    }
}
