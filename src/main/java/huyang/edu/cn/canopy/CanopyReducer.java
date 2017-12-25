package huyang.edu.cn.canopy;

import huyang.edu.cn.ClassUtils;
import huyang.edu.cn.ClusterWritable;
import huyang.edu.cn.Vector;
import huyang.edu.cn.VectorWritable;
import huyang.edu.cn.distance.DistanceMeasure;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class CanopyReducer extends Reducer<Text, VectorWritable, Text, ClusterWritable> {
    private final Collection<CanopyCluster> canopies = new ArrayList<CanopyCluster>();

    private CanopyClusterer canopyClusterer;

    @Override
    protected void reduce(Text arg0, Iterable<VectorWritable> values,
                          Context context) throws IOException, InterruptedException {
        for (VectorWritable value : values) {
            Vector point = value.get();
            canopyClusterer.addPointToCanopies(point, canopies);
        }
        for (CanopyCluster canopy : canopies) {
            canopy.computeParameters();
            ClusterWritable clusterWritable = new ClusterWritable();
            clusterWritable.setValue(canopy);
            context.write(new Text(canopy.getIdentifier()), clusterWritable);
        }
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
}
