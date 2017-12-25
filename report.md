## Report ##
##### KMeans #####
![](https://github.com/huyang1/KMeans/blob/master/src/main/resources/images/1.png)
##### Canopy #####
![](https://github.com/huyang1/KMeans/blob/master/src/main/resources/images/2.png)

### 数据处理 ###

1. 程序首先将input 转化为SequenceFile 便于多次迭代.(一个mr)</br>
   ```Java
    InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");//将输入文件序列化
2. 然后随机生成质心（一个mr）</br>
   ```Java
    if (useCanopy) {
            clusters = Canopy.run(conf,directoryContainingConvertedInput,clusters,measure,t1,t2);
        } else {
            clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
        }
3. 运行Kmeans（每次迭代一个MR）</br>
   ```Java
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
4. KMeans-Map</br>
   ```Java
    Vector probabilities = classifier.classify(value.get());
    int maxIndex = ((DenseVector)probabilities).maxValueIndex();
    classifier.train(maxIndex, value.get(),1.0);//el.index为该instance所属的index，el.get为instance权重
5. KMeans-Reduce</br>
   ```Java
     Iterator<ClusterWritable> iter = values.iterator();
    Cluster first = iter.next().getValue(); // there must always be at least one
    while (iter.hasNext()) {
      Cluster cluster = iter.next().getValue();
      first.observe(cluster);
    }
    List<Cluster> models = new ArrayList<>();
    models.add(first);
    classifier = new ClusterClassifier(models, policy);
    classifier.close();
    //计算阙值是否满足要求
    context.write(key, new ClusterWritable(first));

### Cluster计算 ###
由于当样本较大时无法直接存储所有的数据.所以用s0 s1 s2 三个参数表示一个簇.</br>
* s0 : 样本的个数
* s1 : 样本向量累加和
* s2 : 样本向量平方和
</br>簇的质心：s1/s0</br>
簇的半径：sqrt(s2*s0-s1*s1)/s0
