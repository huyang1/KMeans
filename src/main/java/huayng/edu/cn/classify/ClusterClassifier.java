package huayng.edu.cn.classify;

import com.google.common.io.Closeables;
import huayng.edu.cn.*;
import huayng.edu.cn.classify.model.OnlineLearner;
import huayng.edu.cn.policy.ClusteringPolicy;
import huayng.edu.cn.policy.ClusteringPolicyWritable;
import huayng.edu.cn.sequencefile.PathFilters;
import huayng.edu.cn.sequencefile.PathType;
import huayng.edu.cn.sequencefile.SequenceFileDirValueIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * This classifier works with any ClusteringPolicy and its associated Clusters.
 * It is initialized with a policy and a list of compatible clusters and
 * thereafter it can classify any new Vector into one or more of the clusters
 * based upon the pdf() function which each cluster supports.
 * <p/>
 * In addition, it is an OnlineLearner and can be trained. Training amounts to
 * asking the actual model to observe the vector and closing the classifier
 * causes all the models to computeParameters.
 * <p/>
 * Because a ClusterClassifier implements Writable, it can be written-to and
 * read-from a sequence file as a single entity. For sequential and MapReduce
 * clustering in conjunction with a ClusterIterator; however, it utilizes an
 * exploded file format. In this format, the iterator writes the policy to a
 * single POLICY_FILE_NAME file in the clustersOut directory and the models are
 * written to one or more part-n files so that multiple reducers may employed to
 * produce them.
 */
public class ClusterClassifier  implements OnlineLearner, Writable {

  private static final String POLICY_FILE_NAME = "_policy";

  private List<Cluster> models;//该kmeans算法维护的质心

  private ClusteringPolicy policy;

  /**
   * The public constructor accepts a list of clusters to become the models
   *
   * @param models a List<Cluster>
   * @param policy a ClusteringPolicy
   */
  public ClusterClassifier(List<Cluster> models, ClusteringPolicy policy) {
    this.models = models;
    this.policy = policy;
  }

  // needed for serialization/De-serialization
  public ClusterClassifier() {
  }

  @Override
  public void train(int actual, Vector instance) { models.get(actual).observe(instance); }

  @Override
  public void train(long trackingKey, String groupKey, int actual, Vector instance) {
    models.get(actual).observe(instance);
  }

  public void train(int actual, Vector data, double weight) {
    models.get(actual).observe(data, weight);
  }

  @Override
  public void train(long trackingKey, int actual, Vector instance) {
    models.get(actual).observe(instance);
  }

  @Override
  public void close() { policy.close(this); }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(models.size());
    new ClusteringPolicyWritable(policy).write(out);
    for (Cluster cluster : models) {
      cluster.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    models = new ArrayList<>();
    ClusteringPolicyWritable clusteringPolicyWritable = new ClusteringPolicyWritable();
    clusteringPolicyWritable.readFields(in);
    policy = clusteringPolicyWritable.getValue();
    for (int i = 0; i < size; i++) {
      Cluster element = new DistanceMeasureCluster();
      element.readFields(in);
      models.add(element);
    }
  }

  public List<Cluster> getModels() {
    return models;
  }

  public ClusteringPolicy getPolicy() {
    return policy;
  }

  public Vector classify(Vector instance) {
    return policy.classify(instance, this);
  }

  public void writeToSeqFiles(Path path) throws IOException {
    writePolicy(policy, path);
    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(path.toUri(), config);
    ClusterWritable cw = new ClusterWritable();
    for (int i = 0; i < models.size(); i++) {
      try (SequenceFile.Writer writer = new SequenceFile.Writer(fs, config,
              new Path(path, "part-" + String.format(Locale.ENGLISH, "%05d", i)), IntWritable.class,
              ClusterWritable.class)) {
        Cluster cluster = models.get(i);
        cw.setValue(cluster);
        Writable key = new IntWritable(i);
        writer.append(key, cw);
      }
    }
  }

  public void readFromSeqFiles(Configuration conf, Path path) throws IOException {
    Configuration config = new Configuration();
    List<Cluster> clusters = new ArrayList<>();
    for (ClusterWritable cw : new SequenceFileDirValueIterable<ClusterWritable>(path, PathType.LIST,
            PathFilters.logsCRCFilter(), config)) {
      Cluster cluster = cw.getValue();
      cluster.configure(conf);
      clusters.add(cluster);
    }
    this.models = clusters;
    this.policy = readPolicy(path);
  }

  public static ClusteringPolicy readPolicy(Path path) throws IOException {
    Path policyPath = new Path(path, POLICY_FILE_NAME);
    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(policyPath.toUri(), config);
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, policyPath, config);
    Text key = new Text();
    ClusteringPolicyWritable cpw = new ClusteringPolicyWritable();
    reader.next(key, cpw);
    Closeables.close(reader, true);
    return cpw.getValue();
  }

  public static void writePolicy(ClusteringPolicy policy, Path path) throws IOException {
    Path policyPath = new Path(path, POLICY_FILE_NAME);
    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(policyPath.toUri(), config);
    SequenceFile.Writer writer = new SequenceFile.Writer(fs, config, policyPath, Text.class,
            ClusteringPolicyWritable.class);
    writer.append(new Text(), new ClusteringPolicyWritable(policy));
    Closeables.close(writer, false);
  }

}
