package huayng.edu.cn.Task;

import huayng.edu.cn.HadoopUtil;
import huayng.edu.cn.RandomSeedGenerator;
import huayng.edu.cn.conversion.InputDriver;
import huayng.edu.cn.distance.DistanceMeasure;
import huayng.edu.cn.distance.EuclideanDistanceMeasure;
import huayng.edu.cn.kmeans.KMeansDriver;
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
            run(conf, new Path("testdata"), output, new EuclideanDistanceMeasure(), 6, 0.5, 10);
        }
    }
    public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure, int k,
                           double convergenceDelta, int maxIterations) throws Exception {
        Path directoryContainingConvertedInput = new Path(output, DIRECTORY_CONTAINING_CONVERTED_INPUT);
        log.info("Preparing Input");
        HadoopUtil.delete(conf, directoryContainingConvertedInput);
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");//将输入文件序列化
        log.info("Running random seed to get initial clusters");
        Path clusters = new Path(output, "random-generator-seeds");
        clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
        log.info("Running KMeans with k = {}", k);
        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, convergenceDelta, maxIterations, true, 0.0, false);
//        // run ClusterDumper
//        Path outGlob = new Path(output, "clusters-*-final");
//        Path clusteredPoints = new Path(output,"clusteredPoints");
//        log.info("Dumping out clusters from clusters: {} and clusteredPoints: {}", outGlob, clusteredPoints);
//        ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
//        clusterDumper.printClusters(null);
    }




}
