package hydra_helper;

import java.util.List;

import hydra_helper.*;

public class ColumnData {
	public List<OrderedCluster> clusters;
	public WeightedRandomPicker<OrderedCluster> picker;
	public boolean comparable;
	
	public ColumnData(List<OrderedCluster> clusters, WeightedRandomPicker<OrderedCluster> picker, boolean comparable) {
		this.clusters = clusters;
		this.picker = picker;
		this.comparable = comparable;
	}
}