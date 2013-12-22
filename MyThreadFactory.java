import java.util.concurrent.ThreadFactory;
public class MyThreadFactory implements ThreadFactory{
	int port;
	static int i=0;
   	int random[]={5675,5676, 5677,5678,5679, 5680, 5681, 5682, 5683, 5684};
	public MyThreadFactory()
	{
		this.port=random[i];
		i++;
	}
	@Override
	public Thread newThread(Runnable r)
	{
			return new Thread(r, ""+this.port);
	}
	
}