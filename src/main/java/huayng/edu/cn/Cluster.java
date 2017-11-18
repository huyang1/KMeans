/* Licensed to the Apache Software Foundation (ASF) under one or more
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
package huayng.edu.cn;

import huayng.edu.cn.parameters.Parametered;

import java.util.Map;

/**
 * Implementations of this interface have a printable representation and certain
 * attributes that are common across all clustering implementations
 * 
 */
public interface Cluster extends Parametered {

  
  /**
   * Get the id of the Cluster
   * 
   * @return a unique integer
   */
  int getId();
  
  /**
   * Get the "center" of the Cluster as a Vector
   * 
   * @return a Vector
   */
  Vector getCenter();
  
  /**
   * Get the "radius" of the Cluster as a Vector. Usually the radius is the
   * standard deviation expressed as a Vector of size equal to the center. Some
   * clusters may return zero values if not appropriate.
   * 
   * @return aVector
   */
  Vector getRadius();

  /**
   * Compute a new set of posterior parameters based upon the Observations that
   * have been observed since my creation
   */
  void computeParameters();

  /**
   * Return the number of observations that this model has seen since its
   * parameters were last computed
   * @return
   */
  long getNumObservations();

  /**
   * Return the number of observations that this model has seen over its
   * lifetime
   * @return
   */
  long getTotalObservations();

  /**
   * Observe the given model, retaining information about its observations
   * @return
   */
  void observe(Cluster x);

  void observe(Vector x);

  /**
   * if the receiver has converged, or false if that has no meaning for
   *         the implementation
   * @return
   */
  boolean isConverged();

  /**
   * 判断点在哪个cluster的概率
   * @param x
   * @return
   */
  double pdf(VectorWritable x);

}
