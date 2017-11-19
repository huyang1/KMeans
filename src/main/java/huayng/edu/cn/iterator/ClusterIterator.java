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
package huayng.edu.cn.iterator;

import com.google.common.io.Closeables;
import huayng.edu.cn.Cluster;
import huayng.edu.cn.ClusterWritable;
import huayng.edu.cn.classify.ClusterClassifier;
import huayng.edu.cn.policy.ClusteringPolicy;
import huayng.edu.cn.sequencefile.PathFilters;
import huayng.edu.cn.sequencefile.SequenceFileValueIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * This is a clustering iterator which works with a set of Vector data and a prior ClusterClassifier which has been
 * initialized with a set of models. Its implementation is algorithm-neutral and works for any iterative clustering
 * algorithm (currently k-means and fuzzy-k-means) that processes all the input vectors in each iteration.
 * The cluster classifier is configured with a ClusteringPolicy to select the desired clustering algorithm.
 */
public final class ClusterIterator {
  
  public static final String PRIOR_PATH_KEY = "org.apache.mahout.clustering.prior.path";

  private ClusterIterator() {
  }

  public static void iterateMR(Configuration conf, Path inPath, Path priorPath, Path outPath, int numIterations)
    throws IOException, InterruptedException, ClassNotFoundException {
    ClusteringPolicy policy = ClusterClassifier.readPolicy(priorPath);//选举策略
    Path clustersOut = null;
    int iteration = 1;
    while (iteration <= numIterations) {
      conf.set(PRIOR_PATH_KEY, priorPath.toString());
      
      String jobName = "Cluster Iterator running iteration " + iteration + " over priorPath: " + priorPath;
      Job job = new Job(conf, jobName);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(ClusterWritable.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(ClusterWritable.class);
      
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      job.setMapperClass(CIMapper.class);
      job.setReducerClass(CIReducer.class);
      
      FileInputFormat.addInputPath(job, inPath);
      clustersOut = new Path(outPath, Cluster.CLUSTERS_DIR + iteration);
      priorPath = clustersOut;
      FileOutputFormat.setOutputPath(job, clustersOut);
      
      job.setJarByClass(ClusterIterator.class);
      if (!job.waitForCompletion(true)) {
        throw new InterruptedException("Cluster Iteration " + iteration + " failed processing " + priorPath);
      }
      ClusterClassifier.writePolicy(policy, clustersOut);
      FileSystem fs = FileSystem.get(outPath.toUri(), conf);
      iteration++;
      if (isConverged(clustersOut, conf, fs)) {
        break;
      }
    }
    Path finalClustersIn = new Path(outPath, Cluster.CLUSTERS_DIR + (iteration - 1) + Cluster.FINAL_ITERATION_SUFFIX);
    FileSystem.get(clustersOut.toUri(), conf).rename(clustersOut, finalClustersIn);
  }
  
  /**
   * Return if all of the Clusters in the parts in the filePath have converged or not
   * 
   * @param filePath
   *          the file path to the single file containing the clusters
   * @return true if all Clusters are converged
   * @throws IOException
   *           if there was an IO error
   */
  private static boolean isConverged(Path filePath, Configuration conf, FileSystem fs) throws IOException {
    for (FileStatus part : fs.listStatus(filePath, PathFilters.partFilter())) {
      SequenceFileValueIterator<ClusterWritable> iterator = new SequenceFileValueIterator<>(
          part.getPath(), true, conf);
      while (iterator.hasNext()) {
        ClusterWritable value = iterator.next();
        if (!value.getValue().isConverged()) {
          Closeables.close(iterator, true);
          return false;
        }
      }
    }
    return true;
  }
}
