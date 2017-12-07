## 基于Mahout的CanopyKMeans实现 ##

#### InputDriver ####

实现将inputPath转化为SequenceFile.

#### generator Seed ####

* 随机数生成K个质心
* 利用Canopy生成K个质心，便于迭代收敛。

#### KMeansDriver ####

进行KMeans迭代，设置阈值条件。

#### DisplayKmeans ####

主要用于生成可视化的数据与簇结果。将result flush至磁盘，或进行数据I/O读取。

### 使用参数 ###

    -h      help                     print help message
	-i      inputFile                KMeans input Dir or File path, default is /user/root/testdata
	-o      outputFile               KMeans output Dir path,default is /user/root/output on HDFS.
	-k      clusters                 KMeans cluster number,if not clusters default 3
	-delta  convergenceDelta         KMeans convergenceDelta number if not,convergenceDelta default 0.001.
	-it     maxIterations            Kmeans max iterations, default is 10
	-c      useCanopy                default is not use canopy generator seed
	-t1     T1                       default is 3.0
	-t2     T2                       default is 1.0
