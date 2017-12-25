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
3. 运行Kmeans（每次迭代一个MR）</br>
4. KMeans-Map</br>
5. KMeans-Reduce</br>
