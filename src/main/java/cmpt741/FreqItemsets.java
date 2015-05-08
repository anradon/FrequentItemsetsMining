package cmpt741;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mortbay.log.Log;
import org.paukov.combinatorics.*;

public class FreqItemsets {
    //private static Double percentSupport = 1.0;
    //private static double support = 0;
    //private static int numSplits = 1;
    //private static int maxItemsetSize = 0;
    public static class Mapper1 extends Mapper<Object, BytesWritable, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static int numSplits = 0;
        private static double support = 0;
        //private static double percentSupport = 0;

        Database db = null;
        public List<List<Integer>> chunk =  new ArrayList<List<Integer>>();

        public void setup(Context context) 
        {
            // Get numSplits and percentSupport 
        
            Configuration conf = context.getConfiguration();
            numSplits = Integer.parseInt(conf.get("numSplits"));
            //percentSupport = Double.parseDouble(conf.get("percentSupport"));
            support = Double.parseDouble(conf.get("support"));
        }

        public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException 
        {
           // Configuration conf = context.getConfiguration(); 

            String str = new String(value.copyBytes(), "UTF-8");
            String[] buckets= str.split("\n");
            Log.info(buckets.length+" baskets handed to this map task");

            for(String s : buckets)
            {
                List<Integer> eachLine = new ArrayList<Integer>();
                for(String item : s.trim().split(" "))
                {
                    eachLine.add(Integer.parseInt(item));
                }
                chunk.add(eachLine);
            }

            try
            {
                db = new Database(chunk);
            } 
            catch(Exception e) 
            {
                e.printStackTrace();
            }
            
            double minsup = Math.ceil(1.0/(double)numSplits * support);
            
            Log.info("Minsup: "+Double.toString(minsup));
            Log.info("Mapper 1 : Apriori");
            Apriori test1 = new Apriori("test1", db, minsup); // 1 means 1% minsup
            //Apriori.debugger = true;
            test1.start();
            try 
            {
                test1.join();

                Log.info("Size: "+ Integer.toString(test1.frequent.size()));

                for(List<Integer> pattern : test1.frequent) 
                {
                    StringBuilder sb = new StringBuilder();
                    int c = 0;
                    for(Integer i: pattern)
                    {
                        if(c != 0 ) 
                            sb.append(",");
                        sb.append(i.toString());
                        c++;
                    }
                    word.set(sb.toString());
                    context.write(word,one);
                }
            } 
            catch(Exception e) 
            {
                e.printStackTrace();
            }
        }
    }
    public static class Reducer1 extends Reducer<Text,IntWritable,Text,IntWritable> 
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            result.set(1);
            context.write(key, result);
        }
    }

    public static class Mapper2 extends Mapper<Object, BytesWritable, Text, IntWritable>
    {
        private HashSet<String> candidateItemset = new HashSet<String>();
        private HashMap<String,Integer> freqItemsets = new HashMap<String,Integer>();
        private Text word = new Text();
        private static int maxItemsetSize = 0;

        public void setup(Context context)
        {
            // Read the output from the temp dir for Reducer1
            try 
            {
                FileSystem fs = FileSystem.get(context.getConfiguration());            
                FileStatus[] status = fs.listStatus(new Path("fqtemp"));
                for (int i=0;i<status.length;i++){
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                    String line;
                    line=br.readLine();
                    while (line != null){
                        String itemsets = line.trim().split("\t")[0];
                        String[] items = itemsets.split(",");

                        // Set the max itemset size
                        maxItemsetSize = items.length > maxItemsetSize ? items.length : maxItemsetSize;

                        Arrays.sort(items);
                        StringBuilder sb = new StringBuilder();
                        int c = 0;
                        // Get string key (append ,) for itemsets
                        for(String item : items)
                        {
                            if( c!=0 )
                                sb.append(",");
                            sb.append(item);
                            c++;
                        }
                        candidateItemset.add(sb.toString());
                        line=br.readLine();
                    }
                }
            }
            catch (Exception e)
            {
                Log.info(e.toString());
            }
            Log.info("Size of candidateItemset: "+candidateItemset.size());
        }

        public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException 
        {
            // Read entire file and split per line
            String str = new String(value.copyBytes(), "UTF-8");
            String[] buckets = str.split("\n");
            Log.info(buckets.length+" baskets handed to this map task");


            Integer count = 0;
            for(String s : buckets)
            {
                //Log.info("***** Bucket Count: "+count.toString());
                count++;

                // Split line into basket of items
                //String[] basket = s.trim().split(" ");

                HashSet<String> basket = new HashSet<String>(Arrays.asList(s.trim().split(" ")));

                // Create the initial vector
                //ICombinatoricsVector<String> initialVector = Factory.createVector(basket);
                
                for(String c : candidateItemset)
                {
                    String[] cItems = c.split(",");
                    int notFound = cItems.length;

                    //Log.info("not Found" + cItems.length);

                    for(String cit : cItems)
                    {
                        for(String bItem : basket)
                        {
                            if(cit.equals(bItem))
                                notFound--;
                        }
                    }

                    if(notFound == 0)
                    {
                        //Log.info("Found Frequent: "+c);
                        // It was frequent in the first pass 
                        if(freqItemsets.containsKey(c))
                            freqItemsets.put(c, freqItemsets.get(c)+1);
                        else
                            freqItemsets.put(c,1);
                    }

                }

                // For each length from 1 to size of basket
                // generate combinations and check map
                //for(int i = 1; i <= maxItemsetSize; i++)
                //{
                    //// Create a simple combination generator to generate 3-combinations of the initial vector
                    //Generator<String> gen = Factory.createSimpleCombinationGenerator(initialVector, i);

                    //// Return all possible combinations
                    //for (ICombinatoricsVector<String> combination : gen) {

                        //String[] itemset = combination.getVector().toArray(new String[0]);

                        //Arrays.sort(itemset);
                        //StringBuilder sb = new StringBuilder();
                        //int c = 0;
                        //// Get string key (append ,) for itemsets
                        //for(String item : itemset)
                        //{
                            //if( c!=0 )
                                //sb.append(",");
                            //sb.append(item);
                            //c++;
                        //}
                        //String skey = sb.toString();
                        //if(candidateItemset.contains(skey))
                        //{
                            //// It was frequent in the first pass 
                            //if(freqItemsets.containsKey(skey))
                                //freqItemsets.put(skey, freqItemsets.get(skey)+1);
                            //else
                                //freqItemsets.put(skey,1);
                        //}
                    //}
                //}
            }

            for (Map.Entry<String, Integer> entry : freqItemsets.entrySet()) {
                String k = entry.getKey();
                Integer v = entry.getValue();
                word.set(k);
                context.write(word,new IntWritable(v));
            }
        } 
    }

    public static class Reducer2 extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private IntWritable result = new IntWritable(1);
        private static double support = 0;

        public void setup(Context context) 
        {
            // Get numSplits and percentSupport 
        
            Configuration conf = context.getConfiguration();
            support = Double.parseDouble(conf.get("support"));
        }
        public void reduce(Text key, Iterable<IntWritable>values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if(sum >= support)
            {
                //Log.info("***** key is: "+key+" value is: "+sum + " support is: " +support);
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    private static int countLines(Path path, FileSystem fs) throws IOException 
    {
        int lines = 0;
        FSDataInputStream fis = fs.open(path);
        byte[] buffer = new byte[8192]; // BUFFER_SIZE = 8 * 1024
        int read;

        while ((read = fis.read(buffer)) != -1) {
            for (int i = 0; i < read; i++) {
                if (buffer[i] == '\n') lines++;
            }
        }

        fis.close();
        return lines;
    }


    public static void main(String[] args) throws Exception 
    {
        int numSplits = 0;
        double percentSupport = 0;
        double support = 0;

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) 
        {
            System.err.println("Usage: FreqItemsets <File> <Splits> <Support>");
            System.exit(2);
        }

        try
        {
            numSplits = Integer.parseInt(otherArgs[1]);
            percentSupport = Double.parseDouble(otherArgs[2]);
        }
        catch (NumberFormatException e)
        {
            System.err.println("Incorrect format for args"+e.toString());
            System.exit(2);
        }

        // Paths that we use
        Path FqInputPath = new Path(otherArgs[0]);
        Path FqSplitInputPath = new Path("fqsplitinput");
        Path FqTempPath = new Path("fqtemp");
        Path FqOutputPath = new Path("fqoutput");

        FileSystem fs = FqSplitInputPath.getFileSystem(conf);

        // Remove temp dirs if already exists
        
        if(fs.exists(FqSplitInputPath))
            fs.delete(FqSplitInputPath, true);

        if(fs.exists(FqTempPath))
            fs.delete(FqTempPath, true);

        if(fs.exists(FqOutputPath))
            fs.delete(FqOutputPath, true);

        fs.mkdirs(FqSplitInputPath);

        // Get number of lines in the input file -- Needed for support calc
        // Set the support to use the Reducer2 
        int numBaskets = countLines(new Path(otherArgs[0]), fs);
        support = Math.ceil((double)percentSupport/100 * numBaskets);
        
        // HDFS File input path
        //String InputFilePath = otherArgs[0];

        // Split the file into k parts, and put it into the input directory
        Integer numLines = numBaskets/numSplits;

        // Open the main file for reading

        try
        {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(otherArgs[0]))));
            for(int i = 0; i < numSplits; i++)
            {
                // Create a new split file
                Path splitFilePath = new Path(FqSplitInputPath + "/Split_" + Integer.toString(i) + ".txt");
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(splitFilePath)));

                int lineCount = 0;
                for(String line; (line = br.readLine()) != null;)
                {
                    bw.write(line);
                    bw.newLine();
                    lineCount++;
                    if(lineCount == numLines) break;
                }

                if(i == numSplits-1)
                {
                    for(String line; (line = br.readLine()) != null;)
                    {
                        bw.write(line);
                        bw.newLine();
                    }
                }

                bw.close();
            }
            br.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.exit(2);
        }


        // Messages to pass to Pass1
        
        conf.set("numSplits", Integer.toString(numSplits));
        conf.set("support", Double.toString(support));
        //conf.set("percentSupport", Double.toString(percentSupport));

        // Configure Pass1 

        Job job = new Job(conf);
        job.setJobName("Pass1");
        job.setJarByClass(FreqItemsets.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setMapperClass(Mapper1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job,FqSplitInputPath);
        FileOutputFormat.setOutputPath(job, FqTempPath);

        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();

        // Messages for Pass2
        
        conf2.set("support", Double.toString(support));

        Job job2 = new Job(conf2);
        job2.setJobName("Pass2");
        job2.setJarByClass(FreqItemsets.class);
        job2.setInputFormatClass(WholeFileInputFormat.class);
        job2.setMapperClass(Mapper2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setReducerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, FqSplitInputPath);
        FileOutputFormat.setOutputPath(job2, FqOutputPath);

        job2.waitForCompletion(true);
    }
}
