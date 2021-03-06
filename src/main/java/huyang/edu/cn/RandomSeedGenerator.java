/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package huyang.edu.cn;

import com.google.common.base.Preconditions;
import huyang.edu.cn.distance.DistanceMeasure;
import huyang.edu.cn.sequencefile.PathFilters;
import huyang.edu.cn.sequencefile.SequenceFileIterable;
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
 * write them to the output file as a {@link DistanceMeasureCluster} representing the
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
      
      FileStatus[] inputFiles = fs.globStatus(inputPathPattern, PathFilters.logsCRCFilter());//文件过滤

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
            DistanceMeasureCluster newCluster = new DistanceMeasureCluster(value.get(), nextClusterId++, measure);
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
