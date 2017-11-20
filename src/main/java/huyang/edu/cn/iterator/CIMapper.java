/*
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

package huyang.edu.cn.iterator;

import huyang.edu.cn.*;
import huyang.edu.cn.classify.ClusterClassifier;
import huyang.edu.cn.policy.ClusteringPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class CIMapper extends Mapper<WritableComparable<?>,VectorWritable,IntWritable,ClusterWritable> {
  
  private ClusterClassifier classifier;
  private ClusteringPolicy policy;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    String priorClustersPath = conf.get(ClusterIterator.PRIOR_PATH_KEY);
    classifier = new ClusterClassifier();
    classifier.readFromSeqFiles(conf, new Path(priorClustersPath));
    policy = classifier.getPolicy();
    //policy.update(classifier);
    super.setup(context);
  }

  @Override
  protected void map(WritableComparable<?> key, VectorWritable value, Context context) throws IOException,
      InterruptedException {
    Vector probabilities = classifier.classify(value.get());
    int maxIndex = ((DenseVector)probabilities).maxValueIndex();
    classifier.train(maxIndex, value.get(),1.0);//el.index为该instance所属的index，el.get为instance权重

  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    List<Cluster> clusters = classifier.getModels();
    ClusterWritable cw = new ClusterWritable();
    for (int index = 0; index < clusters.size(); index++) {
      cw.setValue(clusters.get(index));
      context.write(new IntWritable(index), cw);//把flush写在cleanup中。
    }
    super.cleanup(context);
  }
  
}
