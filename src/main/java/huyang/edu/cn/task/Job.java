package huyang.edu.cn.task;

import huyang.edu.cn.HadoopUtil;
import huyang.edu.cn.RandomSeedGenerator;
import huyang.edu.cn.conversion.InputDriver;
import huyang.edu.cn.display.DisplayClustering;
import huyang.edu.cn.distance.DistanceMeasure;
import huyang.edu.cn.distance.EuclideanDistanceMeasure;
import huyang.edu.cn.kmeans.KMeansDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Job {
    private static final Logger log = LoggerFactory.getLogger(Job.class);
    private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "sequenceFileDir";
    private Job() {
    }
    public static void main(String[] args) throws Exception {
        if(args.length>0) {
            System.out.println("稍后处理");
        } else {
            log.info("Running whit default arguments");
            Path output = new Path("output");
            Configuration conf  = new Configuration();
            HadoopUtil.delete(conf, output);
            run(conf, new Path("testdata"), output, new EuclideanDistanceMeasure(), 3, 0.001, 10);
        }
    }
    public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure, int k,
                           double convergenceDelta, int maxIterations) throws Exception {
        Path directoryContainingConvertedInput = new Path(output, DIRECTORY_CONTAINING_CONVERTED_INPUT);
        log.info("Preparing Input");
        HadoopUtil.delete(conf, directoryContainingConvertedInput);
        //InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");//将输入文件序列化
        DisplayClustering displayClustering = new DisplayClustering();
        displayClustering.generateSamples();
        displayClustering.writeSampleData(directoryContainingConvertedInput);
        log.info("Running random seed to get initial clusters");
        Path clusters = new Path(output, "random-generator-seeds");
        clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
        log.info("Running KMeans with k = {}", k);
        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, convergenceDelta, maxIterations, true, 0.0, false);
        displayClustering.loadClustersWritable(output);
        displayClustering.disPlay();
//        // run ClusterDumper
//        Path outGlob = new Path(output, "clusters-*-final");
//        Path clusteredPoints = new Path(output,"clusteredPoints");
//        log.info("Dumping out clusters from clusters: {} and clusteredPoints: {}", outGlob, clusteredPoints);
//        ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
//        clusterDumper.printClusters(null);
    }




}
