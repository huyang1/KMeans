package huyang.edu.cn.canopy;

import huyang.edu.cn.ClassUtils;
import huyang.edu.cn.ClusterWritable;
import huyang.edu.cn.Vector;
import huyang.edu.cn.VectorWritable;
import huyang.edu.cn.distance.DistanceMeasure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * 进行canopy粗分类
 */
public class Canopy {
    private static final Logger log = LoggerFactory.getLogger(Canopy.class);

    public static Path run(Configuration conf, Path input, Path output, DistanceMeasure measure, double t1, double t2) throws IOException, ClassNotFoundException, InterruptedException {
        conf.set("DistanceMeasureClassName",measure.getClass().getName());
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
    private class CanopyMapper extends Mapper<WritableComparable<?>, VectorWritable, Text, VectorWritable> {

        private final Collection<CanopyCluster> canopies = new ArrayList<CanopyCluster>();

        private CanopyClusterer canopyClusterer;

        @Override
        protected void map(WritableComparable<?> key, VectorWritable point,
                           Context context) throws IOException, InterruptedException {
            canopyClusterer.addPointToCanopies(point.get(), canopies);
        }

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            String meansureClassName = context.getConfiguration().get("DistanceMeasureClassName");
            double t1 = Double.parseDouble(context.getConfiguration().get("T1"));
            double t2 = Double.parseDouble(context.getConfiguration().get("T2"));
            canopyClusterer = new CanopyClusterer(ClassUtils.instantiateAs(meansureClassName,DistanceMeasure.class),t1,t2);
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (CanopyCluster canopy : canopies) {
                canopy.computeParameters();
                    context.write(new Text("centerId"), new VectorWritable(canopy.getCenter()));
            }
            super.cleanup(context);
        }
    }

    private class CanopyReducer extends Reducer<Text, VectorWritable, Text, ClusterWritable> {

        private final Collection<CanopyCluster> canopies = new ArrayList<CanopyCluster>();

        private CanopyClusterer canopyClusterer;

        @Override
        protected void reduce(Text arg0, Iterable<VectorWritable> values,
                              Context context) throws IOException, InterruptedException {
            for (VectorWritable value : values) {
                Vector point = value.get();
                canopyClusterer.addPointToCanopies(point, canopies);
            }
            for (CanopyCluster canopy : canopies) {
                canopy.computeParameters();
                    ClusterWritable clusterWritable = new ClusterWritable();
                    clusterWritable.setValue(canopy);
                    context.write(new Text(canopy.getIdentifier()), clusterWritable);
            }
        }

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            String meansureClassName = context.getConfiguration().get("DistanceMeasureClassName");
            double t1 = Double.parseDouble(context.getConfiguration().get("T1"));
            double t2 = Double.parseDouble(context.getConfiguration().get("T2"));
            canopyClusterer = new CanopyClusterer(ClassUtils.instantiateAs(meansureClassName,DistanceMeasure.class),t1,t2);
        }



    }

}
