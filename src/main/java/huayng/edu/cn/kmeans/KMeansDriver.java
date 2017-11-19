/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package huayng.edu.cn.kmeans;

import huayng.edu.cn.Cluster;
import huayng.edu.cn.classify.ClusterClassifier;
import huayng.edu.cn.policy.ClusteringPolicy;
import huayng.edu.cn.policy.KMeansClusteringPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansDriver {
  
  private static final Logger log = LoggerFactory.getLogger(KMeansDriver.class);
  
  /**
   * Iterate over the input vectors to produce clusters and, if requested, use the results of the final iteration to
   * cluster the input vectors.
   *
   * @param input
   *          the directory pathname for input points
   * @param clustersIn
   *          the directory pathname for initial & computed clusters
   * @param output
   *          the directory pathname for output points
   * @param convergenceDelta
   *          the convergence delta value
   * @param maxIterations
   *          the maximum number of iterations
   * @param runClustering
   *          true if points are to be clustered after iterations are completed
   * @param clusterClassificationThreshold
   *          Is a clustering strictness / outlier removal parameter. Its value should be between 0 and 1. Vectors
   *          having pdf below this value will not be clustered.
   * @param runSequential
   *          if true execute sequential algorithm
   */
  public static void run(Configuration conf, Path input, Path clustersIn, Path output,
    double convergenceDelta, int maxIterations, boolean runClustering, double clusterClassificationThreshold,
    boolean runSequential) throws IOException, InterruptedException, ClassNotFoundException {
    
    // iterate until the clusters converge
    String delta = Double.toString(convergenceDelta);
    if (log.isInfoEnabled()) {
      log.info("Input: {} Clusters In: {} Out: {}", input, clustersIn, output);
      log.info("convergence: {} max Iterations: {}", convergenceDelta, maxIterations);
    }
    Path clustersOut = buildClusters(conf, input, clustersIn, output, maxIterations, delta, runSequential);
    if (runClustering) {
      log.info("Clustering data");
     /*------------------/ clusterData(conf, input, clustersOut, output, clusterClassificationThreshold, runSequential);
     --------------------------------------------------------------
      */
    }
  }

  public static Path buildClusters(Configuration conf, Path input, Path clustersIn, Path output,
                                   int maxIterations, String delta, boolean runSequential) throws IOException,
          InterruptedException, ClassNotFoundException {

    double convergenceDelta = Double.parseDouble(delta);
    List<Cluster> clusters = new ArrayList<>();
    KMeansUtil.configureWithClusterInfo(conf, clustersIn, clusters);

    if (clusters.isEmpty()) {
      throw new IllegalStateException("No input clusters found in " + clustersIn + ". Check your -c argument.");
    }

    Path priorClustersPath = new Path(output, Cluster.INITIAL_CLUSTERS_DIR);
    ClusteringPolicy policy = new KMeansClusteringPolicy(convergenceDelta);
    ClusterClassifier prior = new ClusterClassifier(clusters, policy);
    prior.writeToSeqFiles(priorClustersPath);

    if (runSequential) {
      //ClusterIterator.iterateSeq(conf, input, priorClustersPath, output, maxIterations);
      log.info("not support sequence work,please set runSequential : false");
    } else {
      /*----------------------ClusterIterator.iterateMR(conf, input, priorClustersPath, output, maxIterations);
      ---------------------------------------------------------------*/
    }
    return output;
  }
  


}
