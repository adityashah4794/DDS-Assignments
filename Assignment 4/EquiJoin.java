import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class EquiJoin {
    
    public static class Map extends Mapper<Object, Text, Text, Text>
    {
        private Text tuples = new Text();
        private Text kjoin = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            
            String line = value.toString();
	    String[] val = line.split(",");
            String relation = val[0];
            String keyjoin = val[1];   
            kjoin.set(keyjoin);
            tuples.set(line);
            context.write(kjoin,tuples);   
        } 
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text>
    {  
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
        {
            
            List<String> firstTable = new ArrayList<String>();
            List<String> secondTable = new ArrayList<String>();
	    List<String> writableObj = new ArrayList<String>();
            String firstTableName = "";
            Text result = new Text();
            String res = new String();
	    boolean flag = true;
            
            for (Text each : values)
            {
                String value = each.toString();
                String[] valueSplit = value.split(",");
                if (flag == true) 
                {
                    firstTableName = valueSplit[0];
		    flag = false;
                }
                if (firstTableName == valueSplit[0] ) {
                    firstTable.add(value);
                }
                else 
                {
                    secondTable.add(value);
                }
                writableObj.add(value);   
            }
	    Collections.reverse(writableObj);
            Text temp = new Text("");
	    if ( firstTable.size() == 0 || secondTable.size() == 0)
	    {
	    	key.clear();
	    }
	    else
            {
            	for (int i =0; i<writableObj.size(); i++) 
            	{
                	for (int j=i+1; j<writableObj.size(); j++) 
                	{
                    		res = writableObj.get(i) + ", " + writableObj.get(j);
                    		result.set(res);
                    		context.write(temp,result);              
                	}  
                
            	}  
            }
        }
    }
    

    public static void main(String[] args) throws Exception
    {
         Configuration conf = new Configuration();
         Job job = Job.getInstance(conf, "EquiJoin");
         job.setJarByClass(EquiJoin.class);
         job.setMapperClass(Map.class);
         job.setReducerClass(Reduce.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
         FileInputFormat.addInputPath(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));
         System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
