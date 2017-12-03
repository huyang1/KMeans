package huyang.edu.cn.task;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class AbstractJob extends Configured implements Tool{

    private static final Logger log = LoggerFactory.getLogger(AbstractJob.class);



    private Map<String,String> argMap;

    protected final Options options;

    protected AbstractJob() {
        options = new Options();
        argMap = new HashMap<String,String>();
    }

    protected void addOption(String name, String shortName, String description) {
        options.addOption(shortName,name,true,description);
    }

    public Map<String, String> parseArguments(String[] args)
            throws Exception {
        options.addOption(new Option("h","help",false,"print help message"));
        final CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        String param = null;
        try {
            cmd = parser.parse(options, args);
        } catch (final ParseException e) {
            throw new Exception("parser command line error,params error Please -h  get help",e);
        }
        if (cmd.hasOption("h")) {
            argMap.put("help",cmd.getOptionValue("h"));
        } else if (cmd.hasOption("i")) {
            argMap.put("inputPath",cmd.getOptionValue("i"));
        } else if (cmd.hasOption("o")) {
            argMap.put("outputPath",cmd.getOptionValue("o"));
        } else if (cmd.hasOption("k")) {
            argMap.put("clusters",cmd.getOptionValue("k"));
        } else if (cmd.hasOption("delta")) {
            argMap.put("convergenceDelta",cmd.getOptionValue("delta"));
        } else if (cmd.hasOption("it")) {
            argMap.put("maxIterations",cmd.getOptionValue("it"));
        } else if (cmd.hasOption("c")) {
            argMap.put("maxIterations",cmd.getOptionValue("c"));
        } else if (cmd.hasOption("t1")) {
            argMap.put("maxIterations",cmd.getOptionValue("t1"));
        } else if (cmd.hasOption("t2")) {
            argMap.put("maxIterations",cmd.getOptionValue("t2"));
        }
        if (argMap.size()==1 && argMap.containsKey("help")) {
            System.out.printf("%-8s%-25s","  -"+options.getOption("h").getOpt(),options.getOption("h").getLongOpt());
            System.out.println(options.getOption("h").getDescription());
            System.out.printf("%-8s%-25s","  -"+options.getOption("i").getOpt(),options.getOption("i").getLongOpt());
            System.out.println(options.getOption("i").getDescription());
            System.out.printf("%-8s%-25s","  -"+options.getOption("o").getOpt(),options.getOption("o").getLongOpt());
            System.out.println(options.getOption("o").getDescription());
            System.out.printf("%-8s%-25s","  -"+options.getOption("k").getOpt(),options.getOption("k").getLongOpt());
            System.out.println(options.getOption("k").getDescription());
            System.out.printf("%-8s%-25s","  -"+options.getOption("delta").getOpt(),options.getOption("delta").getLongOpt());
            System.out.println(options.getOption("delta").getDescription());
            System.out.printf("%-8s%-25s","  -"+options.getOption("it").getOpt(),options.getOption("it").getLongOpt());
            System.out.println(options.getOption("it").getDescription());
            System.out.printf("%-8s%-25s","  -"+options.getOption("c").getOpt(),options.getOption("c").getLongOpt());
            System.out.println(options.getOption("c").getDescription());
            System.out.printf("%-8s%-25s","  -"+options.getOption("t1").getOpt(),options.getOption("t1").getLongOpt());
            System.out.println(options.getOption("t1").getDescription());
            System.out.printf("%-8s%-25s","  -"+options.getOption("t2").getOpt(),options.getOption("t2").getLongOpt());
            System.out.println(options.getOption("t2").getDescription());
        }
        return argMap;

    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        Configuration result = super.getConf();
        if(result==null) {
            return new Configuration();
        }
        return result;
    }
}
