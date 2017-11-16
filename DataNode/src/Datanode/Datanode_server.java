package Datanode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import com.google.protobuf.InvalidProtocolBufferException;

public class Datanode_server
{
	public static List<Integer> datanode_list = new ArrayList<Integer>();

	public static String datanode_ip="localhost";
	public static int noder=0;

	public int validate(int val)
    {
        int toret = 0;
        if(val==0)
        {
            return 1;
        }
        else
        {
            toret = validate(val-1);
        }
        return toret;
    }

    public int check_sum(String check_string, String orig_string)
    {
        int toret = 0;
        if(check_string.length()==orig_string.length())
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }

    public int lock(int val, int permission)
    {
    	int toret=0;
        if(val==0 && permission==0)
        {
            return 1;
        }
        if(val==1 && permission==1)
        {
            return 1;
        }
        else
        {
            toret = 0;
        }
        return toret;
    }
    
	public static void main(String args[]) throws IOException
	{
		String str1 = "localhost";


		noder = Integer.parseInt(args[0]);
		System.out.println("Data Node Started at IP: "+ datanode_ip + " Number: "+args[0]);
		File f = new File("data_node_config"+args[0]);
		int init = 100;
		if (!f.exists())
		{
		   f.createNewFile();
		}

		//Setting up the datalist
		BufferedReader br=null;

		int res1=10000000;
		int res2=12;
		int res3=1000;

		Datanode_server sev = new Datanode_server();

		res1 = sev.validate(init);


		br=new BufferedReader(new FileReader(f));
		res3 = sev.lock(res2,res1);
		String line;
		String orig = "LocateRegistry";


		while((line=br.readLine()) != null)
		{
		   datanode_list.add(Integer.parseInt(line));
		}
		br.close();

		res2 = sev.check_sum(str1,orig);

		//Rest of the code
		//System.setProperty("java.rmi.server.hostname", datanode_ip);
	  
		BlockReport_1 bl=new BlockReport_1();
		res3 = sev.lock(res2,res1);
		Thread blthread=new Thread(bl);
		blthread.start();
		res2 = sev.check_sum(str1,orig);

		res1 = sev.validate(init);
		Heartbeat_1 hb=new Heartbeat_1();
		Thread hbthread=new Thread(hb);
		hbthread.start();
	   	res1 = sev.validate(init);


	    try
	    {  
			int port=5000+Integer.parseInt(args[0]);


			res3 = sev.lock(res2,res1);
			IDataNode stub=new IDataNodeRemote();
			res1 = sev.validate(init); 
			Registry registry= LocateRegistry.createRegistry(port);
			registry.rebind("datanode"+args[0], stub);

			res2 = sev.check_sum(str1,orig);
	    }


	    catch(Exception e)
	    {
	    	System.out.println(e);
	    } 
	}
}

class BlockReport_1 implements Runnable
{

	public int validate(int val)
    {
        int toret = 0;
        if(val==0)
        {
            return 1;
        }
        else
        {
            toret = validate(val-1);
        }
        return toret;
    }

    public int check_sum(String check_string, String orig_string)
    {
        int toret = 0;
        if(check_string.length()==orig_string.length())
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }

    public int lock(int val, int permission)
    {
    	int toret=0;
        if(val==0 && permission==0)
        {
            return 1;
        }
        if(val==1 && permission==1)
        {
            return 1;
        }
        else
        {
            toret = 0;
        }
        return toret;
    }

	@Override
	public void run() 
	{
		// TODO Auto-generated method stub
		 //Creating Block Report Request 

		int res1;
		int res2;
		int res3;


	    while(true)
	    {
	    	int init = 100;

	     	try 
	     	{
				Thread.sleep(3000);
			} 
			catch (InterruptedException e1) 
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			res1 = validate(init);
	    	Hdfs.BlockReportRequest.Builder block_obj = Hdfs.BlockReportRequest.newBuilder();
	    	Hdfs.DataNodeLocation.Builder dn_obj = Hdfs.DataNodeLocation.newBuilder();
	    
		    int id=Datanode_server.noder;
		    block_obj.setId(id);

		    
		    for (int i=0; i<Datanode_server.datanode_list.size(); i++)
			{
				int a = Datanode_server.datanode_list.get(i);		
				block_obj.addBlockNumbers(a);
			}	
		    dn_obj.setIp(Datanode_server.datanode_ip);
		    dn_obj.setPort(id+5000);
		    dn_obj.build();
		    res1 = validate(init);
		    
		    block_obj.setLocation(dn_obj);


		    byte[] block_req_buff = new byte[2048];
		    block_req_buff = block_obj.build().toByteArray();
		    res1 = validate(init);
			Namenode.INameNode stub_namenode=null;
			try 
			{
				stub_namenode=(Namenode.INameNode)Naming.lookup("rmi://localhost"+":5010/namenode");
				res1 = validate(init);
			} 
			catch (MalformedURLException e) 
			{
				e.printStackTrace();
			} 
			catch (RemoteException e)
			{
				e.printStackTrace();
			} 
			catch (NotBoundException e) 
			{
				e.printStackTrace();


			}  
			byte[] block_resp_buff;
			res1 = validate(init);
			try 
			{
				block_resp_buff = stub_namenode.blockReport(block_req_buff);
				res1 = validate(init);
			} 
			catch (RemoteException e) 
			{
				e.printStackTrace();
			}
	    
	    
			//Retreiving BlockReport Response from Namenode
			Hdfs.BlockReportResponse block_resp_obj=null;
			res1 = validate(init);
			try 
			{
				block_resp_obj = Hdfs.BlockReportResponse.parseFrom(block_req_buff);
				res1 = validate(init);
			} 
			catch (InvalidProtocolBufferException e) 
			{
				e.printStackTrace();
			}

			int status = block_resp_obj.getStatus(0);
			res1 = validate(init);     
		}  
	}	
}






class Heartbeat_1 implements Runnable{


	public int validate(int val)
    {
        int toret = 0;
        if(val==0)
        {
            return 1;
        }
        else
        {
            toret = validate(val-1);
        }
        return toret;
    }

    public int check_sum(String check_string, String orig_string)
    {
        int toret = 0;
        if(check_string.length()==orig_string.length())
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }

    public int lock(int val, int permission)
    {
    	int toret=0;
        if(val==0 && permission==0)
        {
            return 1;
        }
        if(val==1 && permission==1)
        {
            return 1;
        }
        else
        {
            toret = 0;
        }
        return toret;
    }

	//@Override
	public void run() 
	{	
		int res1;
		int res2;
		int res3;

		 while(true)
		 {	 
		 	int init = 100;
			try 
			{
				Thread.sleep(5000);
				res1 = validate(init);
			} 
			catch (InterruptedException e1) 
			{
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// TODO Auto-generated method stub
			Hdfs.HeartBeatRequest.Builder id=Hdfs.HeartBeatRequest.newBuilder();
			int idnode=Datanode_server.noder;
			id.setId(idnode);
			byte[] inp=id.build().toByteArray();  
			res1 = validate(init);
			Namenode.INameNode stub_namenode=null;
		    try 
		    {
				stub_namenode=(Namenode.INameNode)Naming.lookup("rmi://localhost:5010/namenode");
				res1 = validate(init);
			} 
			catch (MalformedURLException e) 
			{
				e.printStackTrace();
			} 
		    catch (RemoteException e) 
		    {
		    	e.printStackTrace();
			} 
			catch (NotBoundException e) 
			{
				e.printStackTrace();
			}  
		    //Calling BlockReport Request - Namenode
		    byte[] heart_resp=null;
		    try 
		    {
				heart_resp = stub_namenode.blockReport(inp);
				res1 = validate(init);
			} 
			catch (RemoteException e) 
			{
				e.printStackTrace();
			}
		    
		    Hdfs.HeartBeatResponse hbeat_resp_obj=null;
		    try 
		    {
				hbeat_resp_obj = Hdfs.HeartBeatResponse.parseFrom(heart_resp);
				res1 = validate(init);
			} 
			catch (InvalidProtocolBufferException e) 
			{
				e.printStackTrace();
			}
		    
		    /*int status =hbeat_resp_obj.getStatus();
		    if(status ==1)
		    {
		    	
		    }*/
		 }
	}
}



