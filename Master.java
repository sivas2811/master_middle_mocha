

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;




public class Master {
	
 private long hashMaxValue;
 private String middleWareIP;
	
//Integer-> point on the circle, String-> ServerID(Physical Node). For a server, this 
//  hashmap will have three entries for the three virtual nodes.
 private TreeMap<Double, String> point2Id = new TreeMap<Double, String>();
 
 private TreeMap<String, ArrayList<Double>> id2Point = 
		 new TreeMap<String, ArrayList<Double>>();
 
 private HashMap<String,String> instance2IPMap = new HashMap<String,String>();
 private SortedSet<String> aliveNodes = new TreeSet<String>();
 
 
 
 public Master()
 {
    this.hashMaxValue = Long.MAX_VALUE;
 }
 
 public void startUp (String configFile) throws IOException
 {
	 	//Reading from config file and mapping to a point on the circle
		BufferedReader br = new BufferedReader(new FileReader(configFile));
		    try {
		        
		        String line;
		        while ((line=br.readLine()) != null) {
				//System.out.println("Config:"+ line);
		        	String[] splits = line.split(";");
		        	//System.out.println("Reading Instance:" + splits[0]+"IP:"+
		        	//splits[1]+"from config file");
		        	this.instance2IPMap.put(splits[0], splits[1]);
		        	this.add(splits[0]);
		        }
		    } finally {
		        br.close();
		    }
	        
        this.middleWareIP = "54.226.150.230"; 
        	 
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

/** Given a server ID, this function will map it to three points on a unit circle and store
 *  the same in the hashmap
 *  
 *  @param serverID which uniquely identifies the server
 */
 public void add(String serverID) {
	 if(id2Point.containsKey(serverID)){
		 throw new  IllegalArgumentException("serverID exists already");
	 }
		 
	 else{
		 
	 double[] fiveCandidates = new double[5];
	 String[] arr ={"one","two","three","four","five"};
	 for(int i=0;i<arr.length;i++)
	 {
		 long hashVal = hash64(serverID+arr[i]);
		 double pointOnCircle = (double) Math.abs(hashVal)/hashMaxValue;
		 fiveCandidates[i] = pointOnCircle;
	 }
	 Arrays.sort(fiveCandidates);
	 point2Id.put(fiveCandidates[0],serverID);
	 point2Id.put(fiveCandidates[2],serverID);
	 point2Id.put(fiveCandidates[4],serverID);
	 id2Point.put(serverID, new ArrayList<Double>
	 (Arrays.asList(fiveCandidates[0],fiveCandidates[2],fiveCandidates[4])));
	 }	 
	 
	 aliveNodes.add(serverID);
	 
 }
/*
 * Given a server ID it will remove all three points which the server is mapped to on the 
 * unit circle
 */
 public void remove(String serverID) {
  if(id2Point.containsKey(serverID)){
	  ArrayList<Double> points = this.getValue_id2Pt(serverID);
	  this.id2Point.remove(serverID);
	  for(int i=0;i<points.size();i++)
		  this.point2Id.remove(points.get(i));
  }
 }
/*
 * Given a string which may be a topic or username - it identifies which physical server 
 * should store the information
 */
 public String get(long key) {
	 if(this.id2Point.size()==0){
		 return null;
	 }
	 else{
		 
		 double pointOnCircle = (double) Math.abs(key)/hashMaxValue;
		 //System.out.println("hashed value:"+pointOnCircle);
		 String serverID,instanceID;
		 double serverPointOnCircle=0, firstServerPoint=0;
		 int i=0;
		 do{
			 
		 if(this.point2Id.higherKey(pointOnCircle)!=null)
		 	{
			 serverPointOnCircle = this.point2Id.higherKey(pointOnCircle);
			 if(i==0)
				 firstServerPoint = serverPointOnCircle;
			 instanceID = this.point2Id.higherEntry(pointOnCircle).getValue();
			 serverID = this.instance2IPMap.get(instanceID);
			 
		 	}
		 else
			 {
			 	//System.out.println("here");
				 serverPointOnCircle = this.point2Id.firstKey();
				 //System.out.println("point on circle:"+ serverPointOnCircle);
				 if(i==0)
					 firstServerPoint = serverPointOnCircle;
				 instanceID = this.point2Id.firstEntry().getValue();
				 serverID = this.instance2IPMap.get(instanceID);
				  
			 	}
		 i++;
		 pointOnCircle = serverPointOnCircle;
			 
		 } while( !this.isAlive(serverID) &&
				 (Double.compare(serverPointOnCircle,firstServerPoint)!=0 && i!=1));
		 //System.out.println(serverID);
		 return serverID;
	 }
	 
 }
 
 public String getValue_pt2ID(Double key)
 {
	 return this.point2Id.get(key);
 }
 
 public boolean isAlive(String serverID)
 {
	if(this.aliveNodes.contains(serverID))
		return true;
	else
		return false;
 }
 
 public String middleWareIP(){
	 return this.middleWareIP;
 }
 
 public String getSuccessorServer(long key)
 {
	 double pointOnCircle = (double) Math.abs(key)/hashMaxValue;
	 return this.point2Id.higherEntry(pointOnCircle).getValue();
 }
 public ArrayList<Double> getValue_id2Pt(String key)
 {
	 return this.id2Point.get(key);
 }
 
 public String getIP(String instance)
 {
	 return this.instance2IPMap.get(instance);
 }
  // UP- UT-TC
 
 public static void main(String[] args) throws Exception
 {	

	 Master masterNode = new Master();

	//Start the instances
		masterNode.startUp(args[0]);
		
		//Listen to messages from the middleware
		 ExecutorService executor = Executors.newFixedThreadPool
                 (10, new ThreadFactory(){
                         public Thread newThread(Runnable runnable){
                                 Thread thread = new MyThreadFactory().newThread(runnable);
                                 return thread;
                         }
                 });
		ServerSocket listener = new ServerSocket(9898);
		Socket socket;
		 while(true)
		 {
			
      
			 socket = listener.accept();
			 BufferedReader in = 
						 new BufferedReader(new InputStreamReader(socket.getInputStream()));
			 String input = in.readLine();
			 //System.out.println("Input from middleware:"+input);
			 if(input.equalsIgnoreCase("stop"))
			 	break;
			  Runnable worker = new WorkerThread_Master(input,masterNode, socket);
		        executor.execute(worker);  
			 			 
		 }
		 socket.close();
		 listener.close();
 }

}

/*
 * TO DO
 * 1. Persistent storage for the maps
 * 2. Informing the data nodes of the maps(initially)-> data nodes keep a persistent copy
 * 3. Get heartbeats from data nodes and update the isAliveList
 * 4. Callbacks 
 */
