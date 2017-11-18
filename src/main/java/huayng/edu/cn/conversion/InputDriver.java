
package huayng.edu.cn.conversion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class converts text files containing space-delimited floating point numbers into
 * Mahout sequence files of VectorWritable suitable for input to the clustering jobs in
 * particular, and any Mahout job requiring this input in general.
 * 由于移除main方法，不支持单独作为工具使用，只做函数调用
 */
public final class InputDriver {

  private static final Logger log = LoggerFactory.getLogger(InputDriver.class);
  
  private InputDriver() {
  }
  
  public static void runJob(Path input, Path output, String vectorClassName)
    throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    conf.set("vector.implementation.class.name", vectorClassName);
    Job job = new Job(conf, "Input Driver running over input: " + input);
    log.info(" input file sequence start!");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(VectorWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapperClass(InputMapper.class);   
    job.setNumReduceTasks(0);
    job.setJarByClass(InputDriver.class);
    
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);//序列化的输出
    
    boolean succeeded = job.waitForCompletion(true);
    if (!succeeded) {
      throw new IllegalStateException("Job failed!");
    }
  }
  
}
