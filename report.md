## Report ##
##### KMeans #####
![](https://github.com/huyang1/KMeans/blob/master/src/main/resources/images/1.png)
##### Canopy #####
![](https://github.com/huyang1/KMeans/blob/master/src/main/resources/images/2.png)

### 数据处理 ###

1. 程序首先将input 转化为SequenceFile 便于多次迭代.(一个mr)</br>
    `InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");//将输入文件序列化`
2. 然后随机生成质心（一个mr）</br>
   `if (useCanopy) {
            clusters = Canopy.run(conf,directoryContainingConvertedInput,clusters,measure,t1,t2);
        } else {
            clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
        }`
3. 运行Kmeans（每次迭代一个MR）</br>
  `while (iteration <= numIterations) {
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
    FileSystem.get(clustersOut.toUri(), conf).rename(clustersOut, finalClustersIn);`
4. KMeans-Map</br>
    `Vector probabilities = classifier.classify(value.get());
    int maxIndex = ((DenseVector)probabilities).maxValueIndex();
    classifier.train(maxIndex, value.get(),1.0);//el.index为该instance所属的index，el.get为instance权重`
5. KMeans-Reduce</br>
