package huayng.edu.cn.kmeans;

import com.google.common.base.Preconditions;
import huayng.edu.cn.HadoopUtil;
import huayng.edu.cn.distance.DistanceMeasure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Given an Input Path containing a {@link SequenceFile}, randomly select k vectors and
 * write them to the output file as a {@link org.apache.mahout.clustering.kmeans.Kluster} representing the
 * initial centroid to use.
 *
 * This implementation uses reservoir sampling as described in http://en.wikipedia.org/wiki/Reservoir_sampling
 */
public final class RandomSeedGenerator {
  
  private static final Logger log = LoggerFactory.getLogger(RandomSeedGenerator.class);
  
  public static final String K = "k";
  
  private RandomSeedGenerator() {}

  public static Path buildRandom(Configuration conf, Path input, Path output, int k, DistanceMeasure measure)
    throws IOException {
    return buildRandom(conf, input, output, k, measure, null);
  }

  public static Path buildRandom(Configuration conf,
                                 Path input,
                                 Path output,
                                 int k,
                                 DistanceMeasure measure,
                                 Long seed) throws IOException {

    Preconditions.checkArgument(k > 0, "Must be: k > 0, but k = " + k);
    // delete the output directory
    FileSystem fs = FileSystem.get(output.toUri(), conf);
    HadoopUtil.delete(conf, output);
    Path outFile = new Path(output, "part-randomSeed");
    boolean newFile = fs.createNewFile(outFile);
    if (newFile) {
      Path inputPathPattern;

      if (fs.getFileStatus(input).isDir()) {
        inputPathPattern = new Path(input, "*");
      } else {
        inputPathPattern = input;
      }
      
      FileStatus[] inputFiles = fs.globStatus(inputPathPattern);

      Random random = (seed != null) ? new Random(seed) : new Random();

      List<Text> chosenTexts = new ArrayList<>(k);
      List<ClusterWritable> chosenClusters = new ArrayList<>(k);
      int nextClusterId = 0;

      int index = 0;
      for (FileStatus fileStatus : inputFiles) {
        if (!fileStatus.isDir()) {
          for (Pair<Writable, VectorWritable> record
              : new SequenceFileIterable<Writable, VectorWritable>(fileStatus.getPath(), true, conf)) {
            Writable key = record.getFirst();
            VectorWritable value = record.getSecond();
            Kluster newCluster = new Kluster(value.get(), nextClusterId++, measure);
            newCluster.observe(value.get(), 1);
            Text newText = new Text(key.toString());
            int currentSize = chosenTexts.size();
            if (currentSize < k) {
              chosenTexts.add(newText);
              ClusterWritable clusterWritable = new ClusterWritable();
              clusterWritable.setValue(newCluster);
              chosenClusters.add(clusterWritable);
            } else {
              int j = random.nextInt(index);
              if (j < k) {
                chosenTexts.set(j, newText);
                ClusterWritable clusterWritable = new ClusterWritable();
                clusterWritable.setValue(newCluster);
                chosenClusters.set(j, clusterWritable);
              }
            }
            index++;
          }
        }
      }

      try (SequenceFile.Writer writer =
               SequenceFile.createWriter(fs, conf, outFile, Text.class, ClusterWritable.class)){
        for (int i = 0; i < chosenTexts.size(); i++) {
          writer.append(chosenTexts.get(i), chosenClusters.get(i));
        }
        log.info("Wrote {} Klusters to {}", k, outFile);
      }
    }
    
    return outFile;
  }

}
