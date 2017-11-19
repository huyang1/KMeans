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

package huayng.edu.cn.kmeans;

import huayng.edu.cn.Cluster;
import huayng.edu.cn.ClusterWritable;
import huayng.edu.cn.DistanceMeasureCluster;
import huayng.edu.cn.sequencefile.PathFilters;
import huayng.edu.cn.sequencefile.PathType;
import huayng.edu.cn.sequencefile.SequenceFileDirValueIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

final class KMeansUtil {
  
  private static final Logger log = LoggerFactory.getLogger(KMeansUtil.class);

  private KMeansUtil() {}
  
  /**
   * Create a list of Klusters from whatever Cluster type is passed in as the prior
   * 
   * @param conf
   *          the Configuration
   * @param clusterPath
   *          the path to the prior Clusters
   * @param clusters
   *          a List<Cluster> to put values into
   */
  public static void configureWithClusterInfo(Configuration conf, Path clusterPath, Collection<Cluster> clusters) {
    for (Writable value : new SequenceFileDirValueIterable<>(clusterPath, PathType.LIST,
        PathFilters.partFilter(), conf)) {
      Class<? extends Writable> valueClass = value.getClass();
      if (valueClass.equals(ClusterWritable.class)) {
        ClusterWritable clusterWritable = (ClusterWritable) value;
        value = clusterWritable.getValue();
        valueClass = value.getClass();
      }
      log.debug("Read 1 Cluster from {}", clusterPath);
      
      if (valueClass.equals(DistanceMeasureCluster.class)) {
        // get the cluster info
        clusters.add((DistanceMeasureCluster) value);
      } else {
        throw new IllegalStateException("Bad value class: " + valueClass);
      }
    }
  }
  
}
