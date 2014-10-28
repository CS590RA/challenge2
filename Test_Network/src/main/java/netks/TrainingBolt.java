package main.java.netks;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
public class TrainingBolt extends BaseBasicBolt implements Serializable{
	private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =

    		Logger.getLogger(TrainingBolt.class);
   // private static final ObjectMapper mapper = new ObjectMapper();
    File file = new File("/root/Downloads/documents/train.csv");
    String text,status;
	String[]  str;
	String sip;
	FileWriter f=null,f1=null;
	BufferedWriter b=null,b1=null;
	/*public TrainingBolt()
	{
		text="PROTOCOL \t SOURCE_IP \t DESTINATION_IP \t STATUS \n";
		try
		{
			if(!file.exists())
			{
				file.createNewFile();
			}
			f = new FileWriter(file.getAbsoluteFile(),true);
			b = new BufferedWriter(f);	
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		try 
		{
			b.write(text);
			b.close();
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	*/
	public void execute(Tuple t1, BasicOutputCollector arg1) {
		// TODO Auto-generated method stub
		String d=t1.getString(0);
		str=d.split(" ");		//Split the data
		
		if(str[6].equals("TCP"))
		{
			if(str[18].equals("0.0"))
			{
				status="TCP Connection";
			}
			else
			{
				status="TCP Data Transfer";
			}
		}
		else if(str[6].equals("UDP"))
		{
			if(str[18].equals("0.0"))
			{
				status="UDP Connection";
			}
			else
			{
				status="UDP  Data Transfer";
			}
		}
		else
		{
			status="Unknown";
		}
		if(str[8].contains("10.100.15")||str[13].contains("10.100.15"))
		{
			status="Backup";
		}
		if(str[8].equals("134.193.126.153")||str[13].equals("134.193.126.153"))
		{
			status="Abnormal";
		}
		text=str[6]+"\t"+str[8]+"\t"+str[13]+"\t"+str[18]+"\t"+status+"\n";
		try
		{
			if(!file.exists())
			{
				file.createNewFile();
			}
			f = new FileWriter(file.getAbsoluteFile(),true);
			b = new BufferedWriter(f);	
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		try 
		{
			b.write(text);
			b.close();
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
