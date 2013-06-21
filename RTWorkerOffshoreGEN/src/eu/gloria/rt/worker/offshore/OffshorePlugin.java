package eu.gloria.rt.worker.offshore;

import java.util.List;

import eu.gloria.rt.db.task.TaskProperty;

public abstract class OffshorePlugin {
	
	protected List<TaskProperty> properties;
	
	public OffshorePlugin(){
		
	}
	
	public void init(List<TaskProperty> properties){
		this.properties = properties;
	}

}
