/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop;

/**
 *
 */




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;



public class OperatorMapRedMapper extends Configured implements Tool {

    public static class MapCatalogToPartition extends Mapper<LongWritable, Text, Text, Text> {

        private ArrayList<String> partitions = new ArrayList<>();

        private FileSystem hdfs = null;

        public ArrayList<String> readLinesFromJobFS(Path p) throws Exception {
            ArrayList<String> ls = new ArrayList<>();

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    this.hdfs.open(p)));
            String line;
            line = br.readLine();
            while (line != null) {
                    ls.add(line);
                line = br.readLine();

            }
            return ls;
        }

        private void readPartition(URI uri) throws Exception {

            partitions = readLinesFromJobFS(new Path(uri));

        }

        @Override
        public void setup(Mapper.Context context) {

            try {
          
                hdfs = FileSystem.get(context.getConfiguration());
                URI[] uris = context.getCacheFiles();
                for (URI uri : uris) {
                    this.readPartition(uri);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        }

        //@Override
        public void map(LongWritable key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] words = line.split(",");

            Double ra = Double.parseDouble(words[1]);

            int partitionsSize = partitions.size();
            String partitionId = "";


            for (int i = 0; i < partitionsSize; i++) {
                if (i == partitionsSize -1) {
                    if (ra < 360.0 && ra >= Double.parseDouble(partitions.get(i))) {
                        partitionId = "p_" + (i+1);

                    }
                    if(ra < Double.parseDouble(partitions.get(i)) && ra >= Double.parseDouble(partitions.get(i-1))){
                        partitionId = "p_" + (i);
                    }

                }
                else{
                    if (i == 0) {
                        if (ra < Double.parseDouble(partitions.get(i)) && ra >= 0.0) {
                            partitionId = "p_" + i;
                        }
                        if (ra < Double.parseDouble(partitions.get(i+1)) && ra >= Double.parseDouble(partitions.get(i))) {
                            partitionId = "p_" + (i+1);
                        }

                    }else if (i > 0 && i < partitionsSize - 1) {

                        if (ra < Double.parseDouble(partitions.get(i)) && ra >= Double.parseDouble(partitions.get(i - 1))) {
                            partitionId = "p_" + i;
                        }
                        if (ra < Double.parseDouble(partitions.get(i + 1)) && ra >= Double.parseDouble(partitions.get(i))) {
                            partitionId = "p_" + i;
                        }

                    }
                }
            }
            Text chave = new Text();
            Text valor = new Text();
            chave.set(partitionId);
            valor.set(partitionId+","+line);
            context.write(chave,valor);

        }
    }

    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs)
                .getRemainingArgs();

        Job job = Job.getInstance(getConf());
        
        FileSystem fs = FileSystem.get(new Configuration());
        fs.exists(new Path(args[2]));
        fs.delete(new Path(args[2]), true);
        job.setJarByClass(OperatorMapRedMapper.class); //é necessário(corrigido)

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapCatalogToPartition.class);

        job.setReducerClass(OperatorMapRedReducer.class);

        /**
         * args[3] = Número de Partições ou número de nós
         */
        job.setNumReduceTasks(Integer.valueOf(args[3]));



        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.addCacheFile(new URI (args[1]));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {


        if (args.length < 4) {
            System.out
                    .println("Use: hadoop jar MavenMapReduce.jar arquivoCatalogos.txt arquivoParticioes pastaSaida numeroDeReduces");

            System.out
                    .println("Exemplo: hadoop jar MavenMapReduce.jar /home/vini/catalogReduced.txt /home/vini/partitionReduced.txr /home/vini/saida 3");

        } else {
        ToolRunner.run(new OperatorMapRedMapper(), args);}

    }

}
