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
package huyang.edu.cn.policy;

import huyang.edu.cn.*;
import huyang.edu.cn.classify.ClusterClassifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * This is a simple maximum likelihood clustering policy, suitable for k-means
 * clustering
 * 
 */
public class KMeansClusteringPolicy implements ClusteringPolicy {
  
  public KMeansClusteringPolicy() {
  }
  
  public KMeansClusteringPolicy(double convergenceDelta) {
    this.convergenceDelta = convergenceDelta;
  }
  
  private double convergenceDelta = 0.001;

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(convergenceDelta);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.convergenceDelta = in.readDouble();
  }

  @Override
  public void close(ClusterClassifier posterior) {
    boolean allConverged = true;
    for (Cluster cluster : posterior.getModels()) {
      DistanceMeasureCluster dmluster = (DistanceMeasureCluster) cluster;
      boolean converged = dmluster.calculateConvergence(convergenceDelta);
      allConverged = allConverged && converged;
      cluster.computeParameters();
    }
  }

  @Override
  public Vector classify(Vector data, ClusterClassifier prior) {
    List<Cluster> models = prior.getModels();
    int i = 0;
    Vector pdfs = new DenseVector(models.size());
    for (Cluster model : models) {
      pdfs.set(i++, model.pdf(new VectorWritable((sampleVector) data)));
    }
    return pdfs.divide(pdfs.zSum());//归一化
  }

  @Override
  public Vector select(Vector probabilities) {
    return null;
  }

  @Override
  public double getDelta() {
    return convergenceDelta;
  }
}
