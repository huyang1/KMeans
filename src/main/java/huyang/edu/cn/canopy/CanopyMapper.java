package huyang.edu.cn.canopy;

import huyang.edu.cn.ClassUtils;
import huyang.edu.cn.VectorWritable;
import huyang.edu.cn.distance.DistanceMeasure;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class CanopyMapper extends Mapper<WritableComparable<?>, VectorWritable, Text, VectorWritable>{
    private final Collection<CanopyCluster> canopies = new ArrayList<CanopyCluster>();

    private CanopyClusterer canopyClusterer;

    @Override
    protected void map(WritableComparable<?> key, VectorWritable point,
                       Context context) throws IOException, InterruptedException {
        canopyClusterer.addPointToCanopies(point.get(), canopies);
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        String meansureClassName = context.getConfiguration().get(Canopy.DistanceMeasureClassName);
        double t1 = Double.parseDouble(context.getConfiguration().get("T1"));
        double t2 = Double.parseDouble(context.getConfiguration().get("T2"));
        canopyClusterer = new CanopyClusterer(ClassUtils.instantiateAs(meansureClassName,DistanceMeasure.class),t1,t2);
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (CanopyCluster canopy : canopies) {
            canopy.computeParameters();
            context.write(new Text("centerId"), new VectorWritable(canopy.getCenter()));
        }
        super.cleanup(context);
    }
}
