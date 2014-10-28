package main.java.netks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
//import com.fasterxml.jackson.databind.ObjectMapper;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BandwidthBolt extends BaseBasicBolt
{
	private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =

    		Logger.getLogger(BandwidthBolt.class);
   // private static final ObjectMapper mapper = new ObjectMapper();
    File file = new File("/root/Downloads/documents/out.txt");
    File file2 = new File("/root/Downloads/documents/out2.txt");
    String text,text1;
	String[]  str;
	String sip;
	FileWriter f=null,f1=null;
	BufferedWriter b=null,b1=null;
	Map<String, Double> counts = new HashMap<String,Double>();
	Map<String, Double> sort = new HashMap<String,Double>();
	public void execute(Tuple t1, BasicOutputCollector boc) {
		// TODO Auto-generated method stub

		LOGGER.debug("Filtering network data");
		String d=t1.getString(0);
				str=d.split(" ");		//Split the data

		Double bwf=Double.parseDouble(str[18]);
		sip=str[8];
		sip.concat(" ").concat(str[13]);
        Double count = counts.get(sip); //Count
       
        if(count==null)
        	count = (double) 0;
        
        	count=count+bwf;
        	counts.put(sip, count);
        
        sort = sortByComparator(counts);
        text="\nBandwidth \n"+str[8]+"\t"+str[13]+"\t"+str[18]+"\n"+"counter \t"+count+"\n";
        text1="\nSorted\n";
        for (Entry<String, Double> entry : sort.entrySet()) {
		
        	if(entry.getValue()!=0.0)
        		text1+="\n key : " + entry.getKey()+"\t value : " + entry.getValue();
        }
        try
		{
			if(!file.exists())
			{
				file.createNewFile();
			}
			f = new FileWriter(file.getAbsoluteFile(),true);
			b = new BufferedWriter(f);
			if(!file2.exists())
			{
				file2.createNewFile();
			}
			f1 = new FileWriter(file2.getAbsoluteFile(),true);
			b1 = new BufferedWriter(f1);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		try 
		{
			b.write(text);
			b.close();
			b1.write(text1);
			b1.close();
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//boc.emit(new Values(str[8],str[13],str[18]));
		}
		
		
	private static Map<String, Double> sortByComparator(Map<String, Double> unsortMap) {
		 
		// Convert Map to List
		List<Map.Entry<String, Double>> list = 
			new LinkedList<Map.Entry<String, Double>>(unsortMap.entrySet());
 
		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
			public int compare(Map.Entry<String, Double> o1,
                                           Map.Entry<String, Double> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});
 
		// Convert sorted map back to a Map
		Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
		for (Iterator<Map.Entry<String, Double>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Double> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
 


	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		// TODO Auto-generated method stub
		
		//ofd.declare(new Fields("0","1","2"));
	}

}
