package huyang.edu.cn.canopy;

import huyang.edu.cn.Vector;
import huyang.edu.cn.distance.DistanceMeasure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class CanopyClusterer {

    private static final Logger log = LoggerFactory.getLogger(CanopyClusterer.class);

    private int canopyId;

    private double t1;

    private double t2;

    private DistanceMeasure measure;

    public CanopyClusterer(DistanceMeasure measure, double t1, double t2) {
        this.measure = measure;
        this.t1 = t1;
        this.t2 = t2;
    }

    public void addPointToCanopies(Vector point, Collection<CanopyCluster> canopies) {
        boolean pointStronglyBound = false;
        for(CanopyCluster canopy : canopies) {
            double distance = measure.distance(point,canopy.getCenter());
            if(distance< t1) {
                log.info("Add point: {} to canopy: {}",point.toString(),canopy.getCenter().toString());
                canopy.observe(point);
            }
            pointStronglyBound = pointStronglyBound || distance < t2;
        }
        if(!pointStronglyBound) {
            log.info("new canopy: {}", point.toString());
            canopies.add(new CanopyCluster(point,canopyId++,measure));
        }
    }
}
