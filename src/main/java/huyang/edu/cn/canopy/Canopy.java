package huyang.edu.cn.canopy;

import huyang.edu.cn.ClusterWritable;
import huyang.edu.cn.VectorWritable;
import huyang.edu.cn.distance.DistanceMeasure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 进行canopy粗分类
 */
public class Canopy {
    private static final Logger log = LoggerFactory.getLogger(Canopy.class);
    public static final String DistanceMeasureClassName = "DistanceMeasureClassName";
    public static Path run(Configuration conf, Path input, Path output, DistanceMeasure measure, double t1, double t2) throws IOException, ClassNotFoundException, InterruptedException {
        conf.set(DistanceMeasureClassName,measure.getClass().getName());
        conf.set("T1",String.valueOf(t1));
        conf.set("T2",String.valueOf(t2));
        Job job = new Job(conf,"Canopy Driver running buildClusters over input: "+ input);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapperClass(CanopyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VectorWritable.class);
        job.setReducerClass(CanopyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ClusterWritable.class);
        job.setNumReduceTasks(1);
        job.setJarByClass(Canopy.class);
        FileInputFormat.addInputPath(job,input);
        Path canopyOutput = new Path(output,"canopySeed");
        FileOutputFormat.setOutputPath(job,canopyOutput);
        if(!job.waitForCompletion(true)) {
            throw new InterruptedException("Canopy Job failed processing "+input);
        }
        return canopyOutput;
    }
}
