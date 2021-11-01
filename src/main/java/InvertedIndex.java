import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;


public class InvertedIndex {
    static List<String> filenamelist=new ArrayList<String>();
    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, Text>{

        static enum CountersEnum { INPUT_WORDS }
        private final static IntWritable one=new IntWritable(1);
        private Text word =new Text();
        private boolean caseSensitive;
        private final Set<String> patternsToSkip = new HashSet<String>();
        private final Set<String> stopwordsToSkip= new HashSet<String>();
        static List<String> stoplist=new ArrayList<String>();
        static boolean readstop=false;
        static boolean readfilename=false;
        static int countmap=0;
        @Override
        public void setup(Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            if (conf.getBoolean("wordcount.skip.patterns", true)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
            if(!readstop){
                InputStream is= Objects.requireNonNull(InvertedIndex.class.getClassLoader().getResource("stop-word-list.txt")).openStream();
                Reader reader = new InputStreamReader(is);
                BufferedReader stopbuffer=new BufferedReader(reader);
                String tmp=null;
                while ((tmp=stopbuffer.readLine())!=null){
                    String[] ss =tmp.split(" ");
                    stoplist.addAll(Arrays.asList(ss));
                }
                stopbuffer.close();
                readstop=true;
            }

            if(!readfilename){
                InputStream is_file= Objects.requireNonNull(InvertedIndex.class.getClassLoader().getResource("filename.txt")).openStream();
                Reader reader = new InputStreamReader(is_file);
                BufferedReader stopbuffer=new BufferedReader(reader);
                String tmp=null;
                while ((tmp=stopbuffer.readLine())!=null){
                    String[] ss =tmp.split(" ");
                    filenamelist.addAll(Arrays.asList(ss));
                }
                stopbuffer.close();
                readfilename=true;
            }
        }
        private void parseSkipFile(String fileName) {
            try {
                BufferedReader fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            String line=(caseSensitive)?
                    value.toString():value.toString().toLowerCase();
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, " ");
            }
            line=line.replaceAll("[0-9][0-9]*"," ");
            Map<String, Integer> freqMap = new HashMap<String, Integer>();
            StringTokenizer itr=new StringTokenizer(line);
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String inputFileName = inputSplit.getPath().getName();
            while (itr.hasMoreTokens()){
                String tmpword= itr.nextToken();
                word.set(tmpword.toLowerCase());
                if (!stoplist.contains(word.toString())&&(tmpword.length()>=3)){
                    context.write(word, new Text(inputFileName));
                    countmap+=1;
//                    System.out.println(countmap);
//                    System.out.println(word.toString()+" "+inputFileName);
//                    System.out.println(countmap);
                }
            }
        }
    }
    static int reducecount=0;
    public static class IntSumReducer extends
            Reducer<Text, Text, Text, Text>{
//        一是写入1的问题
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            if (null == values) {
                return;
            }
            HashMap<String,Integer> map = new HashMap<String,Integer>();
            for (Text val : values) {
                if(!map.containsKey(val.toString())){
//                    System.out.println(val.toString());
                    map.put(val.toString(),1);
                }
                else {
                    map.put(val.toString(), map.get(val.toString()) + 1);
                }
            }
            List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                        public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) {
                            return entry2.getValue() - entry1.getValue();
                        }
                    }
            );
            StringBuilder docValueList = new StringBuilder();
            for (int i=0;i<list.size();i++){
                docValueList.append(list.get(i).getKey()+"#"+list.get(i).getValue()+", ");
            }
//            context.write(key, new Text(String.valueOf(reducecount)+" "+docValueList.toString()));
            context.write(key, new Text(docValueList.toString()));
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job=Job.getInstance(conf,"inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }
}