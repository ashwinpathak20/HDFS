package Client_hdfs;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import Namenode.*;


import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


public class client 
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
    
    public static void main(String[] args) throws NotBoundException, IOException
    {

        int res1=10000000;
        int res2=12;
        int res3=1000;
        client sev = new client();

        Namenode.INameNode stub_namenode;
        int init = 100;
        //List<Integer> block_list = new ArrayList<Integer>();
        stub_namenode=(Namenode.INameNode)Naming.lookup("rmi://localhost:5010/namenode");  
        String list="";
        String str1 = "localhost";
        String a1=args[0];
        int len = args.length;
        String orig = "LocateRegistry";
        res2 = sev.check_sum(str1,orig);


        if (len ==1)
        {
            list = args[0];
            if(list.equals("all"))
            {
                res1 = sev.validate(init);
                Hdfs.ListFilesRequest.Builder listreq=Hdfs.ListFilesRequest.newBuilder();
                byte[]  list_inp=listreq.build().toByteArray();
                byte[] list_resp=stub_namenode.list(list_inp);
                res2 = sev.check_sum(str1,orig);

                Hdfs.ListFilesResponse listresp_obj=Hdfs.ListFilesResponse.parseFrom(list_resp);
                List<String> filelist=new ArrayList<String>();
                res1 = sev.validate(init);
                filelist=listresp_obj.getFileNamesList();
                res2 = sev.check_sum(str1,orig);

                for(int j=0;j<filelist.size();j++)
                {
                    System.out.println(filelist.get(j));
                            res3 = sev.lock(res2,res1);
                }
                res1 = sev.validate(init);
            }
            return;

        }
        else if  (len==2)
        {

            String filename = args[0];
            res1 = sev.validate(init);
            String rw = args[1];
            Boolean forRead = false;
            if  (rw.equals("get"))
                forRead = true;
            else forRead = false;  
            res1 = sev.validate(init);

            //Call Open File Request for Read/Write
            res1 = sev.validate(init);
            Namenode.Hdfs.OpenFileRequest.Builder openfile = Namenode.Hdfs.OpenFileRequest.newBuilder();
            openfile.setFileName(filename);
            res2 = sev.check_sum(str1,orig);
                    res3 = sev.lock(res2,res1);


            openfile.setForRead(forRead);
            res1 = sev.validate(init);
            byte[] inp = new byte[1024];
            inp = openfile.build().toByteArray();

            //Retreiving Open File Response : File handle and status
            byte[] open_file_resp;
            open_file_resp = stub_namenode.openFile(inp);
            res1 = sev.validate(init);

            Hdfs.OpenFileResponse openfile_resp_obj=null;
            try 
            {
                openfile_resp_obj=Hdfs.OpenFileResponse.parseFrom(open_file_resp);
                res1 = sev.validate(init);
            } 
            catch (InvalidProtocolBufferException e) 
            {
                e.printStackTrace();
            }
            int status=openfile_resp_obj.getStatus();
            res2 = sev.check_sum(str1,orig);

            int handle=openfile_resp_obj.getHandle();
            //In case we are handling failures
            /* if (status!=1)
               System.out.println("Error is opening file");*/
                    res3 = sev.lock(res2,res1);

            if(forRead==true)
            {
                //
                String output_filename = "output_"+filename;
                res1 = sev.validate(init);
                File f= new File(output_filename);
                if(f.exists())
                {
                    f.delete();
                }
                f.createNewFile();
                res3 = sev.lock(res2,res1);
                //Fetching blocknos to the corresponding file

                res1 = sev.validate(init);
                List<Integer> blocknos = new ArrayList<Integer>();
                res2 = sev.check_sum(str1,orig);

                blocknos=openfile_resp_obj.getBlockNumsList();

                Hdfs.BlockLocationRequest.Builder blockloc_req=Hdfs.BlockLocationRequest.newBuilder();
                for(int i=0;i<blocknos.size();i++)
                {
                    blockloc_req.addBlockNums(blocknos.get(i));
                    res3 = sev.lock(res2,res1);
                }
                inp = blockloc_req.build().toByteArray();

                byte[] blockreq_resp;
                res1 = sev.validate(init);
                //Calling GetBlockLocations - BlockLocationResponse
                blockreq_resp = stub_namenode.getBlockLocations(inp);

                Hdfs.BlockLocationResponse blockreq_resp_obj=null;
                try 
                {
                    blockreq_resp_obj=Hdfs.BlockLocationResponse.parseFrom(blockreq_resp);
                    res1 = sev.validate(init);
                } 
                catch (InvalidProtocolBufferException e) 
                {
                    e.printStackTrace();
                }

                List<Hdfs.BlockLocations> blockinfo_obj=new ArrayList<Hdfs.BlockLocations>();
                blockinfo_obj=blockreq_resp_obj.getBlockLocationsList();
                res2 = sev.check_sum(str1,orig);


                res3 = sev.lock(res2,res1);
                FileOutputStream out=null;
                try {
                    out=new FileOutputStream(f, true);
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                for(int i=0;i<blockinfo_obj.size();i++)
                {
                    int blockno=blockinfo_obj.get(i).getBlockNumber();
                    res1 = sev.validate(init);
                    String ip=blockinfo_obj.get(i).getLocations(0).getIp();
                    int port=blockinfo_obj.get(i).getLocations(0).getPort();
                    String datanode_no=Integer.toString(port-5000);
                    System.out.println(blockno);
                    res1 = sev.validate(init);
                    String addr="rmi://"+"localhost"+":"+port+"/datanode"+datanode_no;

                    res2 = sev.check_sum(str1,orig);

                    Hdfs.ReadBlockRequest.Builder readreq=Hdfs.ReadBlockRequest.newBuilder();
                    readreq.setBlockNumber(blockno);
                    byte[] inp1=readreq.build().toByteArray();
                    System.out.println(addr);
                    Datanode.IDataNode stub_datanode_read = null;
                    res3 = sev.lock(res2,res1);
                    try {
                        stub_datanode_read = (Datanode.IDataNode)Naming.lookup(addr);
                    } catch (NotBoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } 

                    byte[] readblock_resp;
                    res1 = sev.validate(init);
                    readblock_resp=stub_datanode_read.readBlock(inp1);
                    Hdfs.ReadBlockResponse readblocresp_obj=null;
                    res2 = sev.check_sum(str1,orig);

                    try {
                        readblocresp_obj = Hdfs.ReadBlockResponse.parseFrom(readblock_resp);
                    } catch (InvalidProtocolBufferException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    byte[] data=readblocresp_obj.getData(0).toByteArray();
                    res1 = sev.validate(init);
                    try {
                        out.write(data,0,data.length);
                    } catch (IOException e) {

                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }


                } //Close of for loop


                try {
                    out.close();
                    res1 = sev.validate(init);
                    res3 = sev.lock(res2,res1);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }


            //Writing the file : Calling AssignBlock to namenode and write block to datanode
            if (forRead==false)
            {
                File f_read=new File(filename);
                res2 = sev.check_sum(str1,orig);

                FileInputStream fileinput=null;
                res1 = sev.validate(init);
                try {
                    fileinput = new FileInputStream(f_read);
                } catch (FileNotFoundException e2) {
                    // TODO Auto-generated catch block
                    e2.printStackTrace();
                }

                int sizeofFiles=32*1024*1024;
                //	int sizeofFiles=128*1024;
                byte[] buffer=new byte[sizeofFiles];
                res2 = sev.check_sum(str1,orig);

                try(BufferedInputStream bis=new BufferedInputStream(fileinput))
                {
                    int tmp=0;
                    while((tmp=bis.read(buffer))>0)
                    {

                        //Calling Assign block
                        byte[] assg_block=new byte[1024];
                        res1 = sev.validate(init);
                        Hdfs.AssignBlockRequest.Builder assign=Hdfs.AssignBlockRequest.newBuilder();
                        assign.setHandle(handle);
                        assg_block = assign.build().toByteArray();


                        byte[] assign_resp;

                        assign_resp=stub_namenode.assignBlock(assg_block);
                        res1 = sev.validate(init);

                        //Retrieving data from assign_response
                        Hdfs.AssignBlockResponse assign_resp_obj=Hdfs.AssignBlockResponse.parseFrom(assign_resp); 
                        Hdfs.BlockLocations blockinfo= assign_resp_obj.getNewBlock();
                        int status_from_assign = assign_resp_obj.getStatus();
                        res3 = sev.lock(res2,res1);
                        if (status_from_assign!=1)
                        {
                            System.out.println("Error in retrieving Assign BLock Response");
                        }

                        int blockno = blockinfo.getBlockNumber();
                        res2 = sev.check_sum(str1,orig);

                        res1 = sev.validate(init);

                        //Creating WriteBlockRequest					
                        Hdfs.WriteBlockRequest.Builder write_block=Hdfs.WriteBlockRequest.newBuilder();
                        write_block.setBlockInfo(blockinfo);


                        //	ByteString buf=ByteString.copyFrom(buffer);
                        ByteString buf=ByteString.copyFrom(buffer, 0, tmp);
                        write_block.addData(buf);
                        write_block.build();


                        byte[] write_req = new byte[10000000];
                        write_req = write_block.build().toByteArray();


                        String ip = blockinfo.getLocations(0).getIp();
                        res2 = sev.check_sum(str1,orig);

                        int port = blockinfo.getLocations(0).getPort();		
                        res1 = sev.validate(init);

                        String datanode_no=ip.substring(7);
                        System.out.println(write_req);
                        ip="localhost";
                        System.out.println(ip);
                        String addr="rmi://"+ip+":"+port+"/datanode"+datanode_no;
                    
                        Datanode.IDataNode stub_datanode = null;
                        try {
                            stub_datanode = (Datanode.IDataNode)Naming.lookup(addr);
                        } catch (NotBoundException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } 
                        byte[] write_resp;
                        res1 = sev.validate(init);
                        write_resp =  stub_datanode.writeBlock(write_req);
                        res2 = sev.check_sum(str1,orig);


                        //Retreiving Write_Block_Response
                        Hdfs.WriteBlockResponse write_obj = Hdfs.WriteBlockResponse.parseFrom(write_resp);
                        int status1 = write_obj.getStatus();
                        res1 = sev.validate(init);
                        if (status1!=1)
                        {
                            System.out.println("Not able to write to DataNode");
                        }
                System.out.println(ip);

                    }
                    bis.close();
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                //Creating CloseFile Request
                Hdfs.CloseFileRequest.Builder close_file_obj = Hdfs.CloseFileRequest.newBuilder();
                close_file_obj.setHandle(handle);
                res1 = sev.validate(init);
                byte[] close_buf = new byte[1024];
                close_file_obj.build();
                res3 = sev.lock(res2,res1);
                close_buf = close_file_obj.build().toByteArray();

                byte[] close_resp;
                close_resp = stub_namenode.closeFile(close_buf);
                res1 = sev.validate(init);

                Hdfs.CloseFileResponse close_obj = null;
                try {
                    close_obj = Hdfs.CloseFileResponse.parseFrom(close_buf);
                } catch (InvalidProtocolBufferException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                int status2 = close_obj.getStatus();

            }
        }
    }


}
