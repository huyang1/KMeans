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
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class Job extends AbstractJob{
    private static final Logger log = LoggerFactory.getLogger(Job.class);
    private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "sequenceFileDir";
    private Job() {
    }
    public static void main(String[] args) throws Exception {
        if(args.length>0) {
            log.info("Running with only user-supplied arguments");
            ToolRunner.run(new Configuration(), new Job(), args);
        } else {
            log.info("Running whit default arguments");
            Path output = new Path("output");
            Configuration conf  = new Configuration();
            HadoopUtil.delete(conf, output);
            run(conf, output, new EuclideanDistanceMeasure(), 3, 0.001, 10);
        }
    }
    public static void run(Configuration conf, Path output, DistanceMeasure measure, int k,
                           double convergenceDelta, int maxIterations) throws Exception {
        Path directoryContainingConvertedInput = new Path(output, DIRECTORY_CONTAINING_CONVERTED_INPUT);
        log.info("Preparing Input");
        HadoopUtil.delete(conf, directoryContainingConvertedInput);
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
    }

    public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure, int k,
                           double convergenceDelta, int maxIterations) throws Exception {
        HadoopUtil.delete(conf, output);
        Path directoryContainingConvertedInput = new Path(output, DIRECTORY_CONTAINING_CONVERTED_INPUT);
        log.info("Preparing Input");
        HadoopUtil.delete(conf, directoryContainingConvertedInput);
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");//将输入文件序列化
        log.info("Running random seed to get initial clusters");
        Path clusters = new Path(output, "random-generator-seeds");
        clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
        log.info("Running KMeans with k = {}", k);
        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, convergenceDelta, maxIterations, true, 0.0, false);
    }

    @Override
    public int run(String[] args) throws Exception {
        addOption("inputFile","i","KMeans input Dir or File path, if all params not " +
                "inputPath sample random generator, else if just not -i option, the inputPath is /user/root/testdata ");
        addOption("outputFile","o","KMeans output Dir path,if not" +
                "outputPath default is /user/root/output on HDFS.");
        addOption("clusters","k","KMeans cluster number,if not " +
                "clusters default 3");
        addOption("convergenceDelta","delta","KMeans convergenceDelta number if not," +
                "convergenceDelta default 0.001");
        addOption("maxIterations","it","Kmeans max iterations, default is 10");
        Path inputPath;
        Path outputPath;
        int k;
        double convergenceDelta;
        int maxIterations;

        Map<String,String> argMap = parseArguments(args);
        if (argMap == null) {
            return -1;
        } else if (argMap.size()==1 && argMap.containsKey("help")) {
            return 0;
        }
        if(argMap.containsKey("inputPath")) {
            inputPath = new Path(argMap.get("inputPath"));
        } else {
            inputPath = new Path("testdata");
        }
        if(argMap.containsKey("outputPath")) {
            outputPath = new Path(argMap.get("inputPath"));
        } else {
            outputPath = new Path("output");
        }
        if(argMap.containsKey("sclusters")) {
            k = Integer.parseInt(argMap.get("clusters"));
        } else {
            k = 3;
        }
        if(argMap.containsKey("convergenceDelta")) {
            convergenceDelta = Double.parseDouble(argMap.get("convergenceDelta"));
        } else {
            convergenceDelta = 0.01;
        }
        if(argMap.containsKey("maxIterations")) {
            maxIterations = Integer.parseInt(argMap.get("maxIterations"));
        } else {
            maxIterations = 10;
        }
        run(getConf(), inputPath, outputPath, new EuclideanDistanceMeasure(), k, convergenceDelta, maxIterations);
        return 0;
    }

}