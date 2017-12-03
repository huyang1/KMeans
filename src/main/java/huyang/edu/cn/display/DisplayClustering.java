package huyang.edu.cn.display;

import huyang.edu.cn.*;
import huyang.edu.cn.sequencefile.PathFilters;
import huyang.edu.cn.sequencefile.PathType;
import huyang.edu.cn.sequencefile.SequenceFileDirValueIterable;
import huyang.edu.cn.sequencefile.SequenceFileIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.geom.AffineTransform;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DisplayClustering {
  
  private static final Logger log = LoggerFactory.getLogger(DisplayClustering.class);
  
  protected static final int DS = 72; // default scale = 72 pixels per inch
  
  protected static final int SIZE = 8; // screen size in inches
  
  private static  Collection<DenseVector> SAMPLE_PARAMS = new ArrayList<>();
  
  protected static  List<VectorWritable> SAMPLE_DATA = new ArrayList<>();
  
  protected static  List<List<Cluster>> CLUSTERS = new ArrayList<>();
  
  static final Color[] COLORS = { Color.red, Color.orange, Color.yellow, Color.green, Color.blue, Color.magenta,
    Color.lightGray };

  
  protected static int res; // screen resolution
  
  public DisplayClustering() {
  }

  public DisplayClustering(Collection<DenseVector> SAMPLE_PARAMS, List<VectorWritable> SAMPLE_DATA,List<List<Cluster>> CLUSTERS) {
    this.SAMPLE_DATA = SAMPLE_DATA;
    this.SAMPLE_PARAMS = SAMPLE_PARAMS;
    this.CLUSTERS = CLUSTERS;
  }

  public DisplayClustering(Collection<DenseVector> SAMPLE_PARAMS, List<VectorWritable> SAMPLE_DATA) {
    this.SAMPLE_DATA = SAMPLE_DATA;
    this.SAMPLE_PARAMS = SAMPLE_PARAMS;
  }

  public DisplayClustering(List<VectorWritable> SAMPLE_DATA) {
    this.SAMPLE_DATA = SAMPLE_DATA;
  }

   static class Display extends Frame {
    public Display() {
      initialize();
      this.setTitle("Sample Data");
    }
     public void initialize() {
       // Get screen resolution
       res = Toolkit.getDefaultToolkit().getScreenResolution();

       // Set Frame size in inches
       this.setSize(SIZE * res, SIZE * res);
       this.setVisible(true);
       this.setTitle("Asymmetric Sample Data");

       // Window listener to terminate program.
       this.addWindowListener(new WindowAdapter() {
         @Override
         public void windowClosing(WindowEvent e) {
           System.exit(0);
         }
       });
     }

     @Override
     public void paint(Graphics g) {
       Graphics2D g2 = (Graphics2D) g;
       plotSampleData(g2);
       plotSampleParameters(g2);
       plotClusters(g2);
     }
  }
  
  public static void main(String[] args) throws Exception {
    RandomUtils.useTestSeed();
    generateSamples();
    new Display();


  }
  
  // Override the paint() method

  
  protected static void plotClusters(Graphics2D g2) {
    int cx = CLUSTERS.size() - 1;
    for (List<Cluster> clusters : CLUSTERS) {
      g2.setStroke(new BasicStroke(cx == 0 ? 3 : 1));
      g2.setColor(COLORS[2]);
      for (Cluster cluster : clusters) {
        plotEllipse(g2, cluster.getCenter(), cluster.getRadius().multi(1.5));
      }
    }
  }
  
  protected static void plotSampleParameters(Graphics2D g2) {
    Vector v = new DenseVector(2);
    Vector dv = new DenseVector(2);
    for (DenseVector param : SAMPLE_PARAMS) {
      g2.setColor(COLORS[0]);
      v.set(0, param.get(0));
      v.set(1, param.get(1));
      dv.set(0, param.get(2) * 3);
      dv.set(1, param.get(3) * 3);
      plotEllipse(g2, v, dv);
    }
  }
  
  protected static void plotSampleData(Graphics2D g2) {
    double sx = (double) res / DS;
    g2.setTransform(AffineTransform.getScaleInstance(sx, sx));
    
    // plot the axes
    g2.setColor(Color.BLACK);
    Vector dv = new DenseVector(2).assign(SIZE / 2.0);
    plotRectangle(g2, new DenseVector(2).assign(2), dv);
    plotRectangle(g2, new DenseVector(2).assign(-2), dv);
    
    // plot the sample data
    g2.setColor(Color.DARK_GRAY);
    ((DenseVector)dv).assign(0.02);
    for (VectorWritable v : SAMPLE_DATA) {
      plotRectangle(g2, v.get(), dv);
    }
  }
  
  /**
   * Draw a rectangle on the graphics context
   * 
   * @param g2
   *          a Graphics2D context
   * @param v
   *          a Vector of rectangle center
   * @param dv
   *          a Vector of rectangle dimensions
   */
  protected static void plotRectangle(Graphics2D g2, Vector<Double> v, Vector<Double> dv) {
    double[] flip = {1, -1};
    Vector<Double> v2;
    if(v instanceof sampleVector) {
      v2 = ((sampleVector) v).toDenseVector().times(new DenseVector(flip));
    } else {
      v2 = ((DenseVector)v).times(new DenseVector(flip));
    }
    v2 = v2.minus(dv.divide(2));
    int h = SIZE / 2;
    double x = v2.get(0) + h;
    double y = v2.get(1) + h;
    g2.draw(new Rectangle2D.Double(x * DS, y * DS, dv.get(0) * DS, dv.get(1) * DS));
  }
  
  /**
   * Draw an ellipse on the graphics context
   * 
   * @param g2
   *          a Graphics2D context
   * @param v
   *          a Vector of ellipse center
   * @param dv
   *          a Vector of ellipse dimensions
   */
  protected static void plotEllipse(Graphics2D g2, Vector v, Vector<Double> dv) {
    double[] flip = {1, -1};
    Vector<Double> v2;
    if(v instanceof sampleVector) {
      v2 = ((sampleVector) v).toDenseVector().times(new DenseVector(flip));
    } else {
      v2 = ((DenseVector)v).times(new DenseVector(flip));
    }
    v2 = v2.minus(dv.divide(2));
    int h = SIZE / 2;
    double x = v2.get(0) + h;
    double y = v2.get(1) + h;
    g2.draw(new Ellipse2D.Double(x * DS, y * DS, dv.get(0) * DS, dv.get(1) * DS));
  }
  
  public static void generateSamples() {
    generateSamples(500, 1, 1, 2);
    generateSamples(300, 1, 0, 0.5);
    generateSamples(300, 0, 2, 0.1);
  }

  
  /**
   * Generate random samples and add them to the sampleData
   * 
   * @param num
   *          int number of samples to generate
   * @param mx
   *          double x-value of the sample mean
   * @param my
   *          double y-value of the sample mean
   * @param sd
   *          double standard deviation of the samples
   */
  public static void generateSamples(int num, double mx, double my, double sd) {
    double[] params = {mx, my, sd, sd};
    SAMPLE_PARAMS.add( new DenseVector(params));
    log.info("Generating {} samples m=[{}, {}] sd={}", num, mx, my, sd);
    for (int i = 0; i < num; i++) {
      SAMPLE_DATA.add(new VectorWritable(new DenseVector(new double[] {UncommonDistributions.rNorm(mx, sd),
          UncommonDistributions.rNorm(my, sd)})));
    }
  }
  
  public static void writeSampleData(Path output) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(output.toUri(), conf);

    try (SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, output, Text.class, VectorWritable.class)) {
      int i = 0;
      for (VectorWritable vw : SAMPLE_DATA) {
        writer.append(new Text("" + i++), vw.ToSampleVector());
      }
    } catch (Exception e) {
      log.info("DenseVector translation sampleVector fail");
      e.printStackTrace();
    }
  }

  public static void ReadSampleData(Path input) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(input.toUri(), conf);
    FileStatus[] inputFiles = fs.globStatus(input, PathFilters.logsCRCFilter());
    for(FileStatus status : inputFiles) {
      for (Pair<Writable, VectorWritable> record
              : new SequenceFileIterable<Writable, VectorWritable>(status.getPath(), true, conf)) {
        SAMPLE_DATA.add(record.getSecond());
      }
    }

  }
  
  public static void loadClustersWritable(Path output) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(output.toUri(), conf);
    for (FileStatus s : fs.listStatus(output, new ClustersFilter())) {
      List<Cluster> clusters = readClustersWritable(s.getPath());
      CLUSTERS.add(clusters);
    }
  }

  public void disPlay() {
    log.info("display kmeans result!");
    new Display();
  }

}
