package huayng.edu.cn.display;

import huayng.edu.cn.*;
import huayng.edu.cn.sequencefile.PathFilters;
import huayng.edu.cn.sequencefile.PathType;
import huayng.edu.cn.sequencefile.SequenceFileDirValueIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
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

public class DisplayClustering extends Frame {
  
  private static final Logger log = LoggerFactory.getLogger(DisplayClustering.class);
  
  protected static final int DS = 72; // default scale = 72 pixels per inch
  
  protected static final int SIZE = 8; // screen size in inches
  
  private static final Collection<DenseVector> SAMPLE_PARAMS = new ArrayList<>();
  
  protected static final List<VectorWritable> SAMPLE_DATA = new ArrayList<>();
  
  protected static final List<List<Cluster>> CLUSTERS = new ArrayList<>();
  
  static final Color[] COLORS = { Color.red, Color.orange, Color.yellow, Color.green, Color.blue, Color.magenta,
    Color.lightGray };

  
  protected static int res; // screen resolution
  
  public DisplayClustering() {
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
  
  public static void main(String[] args) throws Exception {
    //RandomUtils.useTestSeed();
    generateSamples();
    new DisplayClustering();
  }
  
  // Override the paint() method
  @Override
  public void paint(Graphics g) {
    Graphics2D g2 = (Graphics2D) g;
    plotSampleData(g2);
    plotSampleParameters(g2);
    plotClusters(g2);
  }
  
  protected static void plotClusters(Graphics2D g2) {
    int cx = CLUSTERS.size() - 1;
    for (List<Cluster> clusters : CLUSTERS) {
      g2.setStroke(new BasicStroke(cx == 0 ? 3 : 1));
      g2.setColor(COLORS[Math.min(COLORS.length - 1, cx--)]);
      for (Cluster cluster : clusters) {
        plotEllipse(g2, cluster.getCenter(), cluster.getRadius().multi(3));
      }
    }
  }
  
  protected static void plotSampleParameters(Graphics2D g2) {
    Vector v = new DenseVector(2);
    Vector dv = new DenseVector(2);
    int cx = COLORS.length - 1;
    for (DenseVector param : SAMPLE_PARAMS) {
      g2.setColor(COLORS[cx--]);
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
  protected static void plotRectangle(Graphics2D g2, Vector v, Vector dv) {
    double[] flip = {1, -1};
    Vector v2 = ((DenseVector)v).times(new DenseVector(flip));
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
  protected static void plotEllipse(Graphics2D g2, Vector v, Vector dv) {
    double[] flip = {1, -1};
    Vector v2 = ((DenseVector)v).times(new DenseVector(flip));
    v2 = v2.minus(dv.divide(2));
    int h = SIZE / 2;
    double x = v2.get(0) + h;
    double y = v2.get(1) + h;
    g2.draw(new Ellipse2D.Double(x * DS, y * DS, dv.get(0) * DS, dv.get(1) * DS));
  }
  
  protected static void generateSamples() {
    generateSamples(500, 1, 1, 3);
    generateSamples(300, 1, 0, 0.5);
    generateSamples(300, 0, 2, 0.1);
  }
  
  protected static void generate2dSamples() {
    generate2dSamples(500, 1, 1, 3, 1);
    generate2dSamples(300, 1, 0, 0.5, 1);
    generate2dSamples(300, 0, 2, 0.1, 0.5);
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
  protected static void generateSamples(int num, double mx, double my, double sd) {
    double[] params = {mx, my, sd, sd};
    SAMPLE_PARAMS.add( new DenseVector(params));
    log.info("Generating {} samples m=[{}, {}] sd={}", num, mx, my, sd);
    for (int i = 0; i < num; i++) {
      SAMPLE_DATA.add(new VectorWritable(new DenseVector(new double[] {UncommonDistributions.rNorm(mx, sd),
          UncommonDistributions.rNorm(my, sd)})));
    }
  }
  
  protected static void writeSampleData(Path output) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(output.toUri(), conf);

    try (SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, output, Text.class, VectorWritable.class)) {
      int i = 0;
      for (VectorWritable vw : SAMPLE_DATA) {
        writer.append(new Text("sample_" + i++), vw);
      }
    }
  }
  
  protected static List<Cluster> readClustersWritable(Path clustersIn) {
    List<Cluster> clusters = new ArrayList<>();
    Configuration conf = new Configuration();
    for (ClusterWritable value : new SequenceFileDirValueIterable<ClusterWritable>(clustersIn, PathType.LIST,
        PathFilters.logsCRCFilter(), conf)) {
      Cluster cluster = value.getValue();
      log.info(
          "Reading Cluster:{} center:{} numPoints:{} radius:{}",
          cluster.getId(), cluster.getCenter(),
          cluster.getNumObservations(), cluster.getRadius(), null);
      clusters.add(cluster);
    }
    return clusters;
  }
  
  protected static void loadClustersWritable(Path output) throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(output.toUri(), conf);
    for (FileStatus s : fs.listStatus(output, new ClustersFilter())) {
      List<Cluster> clusters = readClustersWritable(s.getPath());
      CLUSTERS.add(clusters);
    }
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
   * @param sdx
   *          double x-value standard deviation of the samples
   * @param sdy
   *          double y-value standard deviation of the samples
   */
  protected static void generate2dSamples(int num, double mx, double my, double sdx, double sdy) {
    double[] params = {mx, my, sdx, sdy};
    SAMPLE_PARAMS.add(new DenseVector(params));
    log.info("Generating {} samples m=[{}, {}] sd=[{}, {}]", num, mx, my, sdx, sdy);
    for (int i = 0; i < num; i++) {
      SAMPLE_DATA.add(new VectorWritable(new DenseVector(new double[] {UncommonDistributions.rNorm(mx, sdx),
          UncommonDistributions.rNorm(my, sdy)})));
    }
  }

}
