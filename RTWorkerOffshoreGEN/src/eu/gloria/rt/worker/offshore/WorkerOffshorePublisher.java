package eu.gloria.rt.worker.offshore;

import java.lang.reflect.Constructor;
import java.nio.channels.ClosedByInterruptException;

import javax.persistence.EntityManager;

import eu.gloria.rt.db.scheduler.ObservingPlan;
import eu.gloria.rt.db.scheduler.ObservingPlanManager;
import eu.gloria.rt.db.util.DBUtil;
import eu.gloria.rt.worker.core.Worker;
import eu.gloria.rti.sch.core.OffshorePluginPublisher;
import eu.gloria.tools.log.LogUtil;

public class WorkerOffshorePublisher extends Worker {

	@Override
	protected void doAction() throws InterruptedException,
			ClosedByInterruptException, Exception {
		
		boolean verbose = true;
		
		EntityManager em = DBUtil.getEntityManager();
		
		ObservingPlan op = null;
		ObservingPlanManager manager = new ObservingPlanManager();
		
		//Build the plugin
		String provider = getPropertyStringValue("ProviderOffshorePluginPublisher");
		if (provider == null) throw new Exception("The ProviderOffshorePluginPublisher key does not exist into the task properties list.");
		Class<?> cls = Class.forName(provider);
		Constructor<?> ct = cls.getConstructor();
		OffshorePluginPublisher publisher = (OffshorePluginPublisher) ct.newInstance();
		publisher.init(this.getProperties());
		//OffshorePublisher publisher = new OffshorePublisherACP();
		
		try{
			
			DBUtil.beginTransaction(em);
			
			op = manager.getNextToProcess(em);
			
			if (op != null){
				
				publisher.publish(op.getId());
				
			}else{
				
				if (verbose) LogUtil.info(this, "PROCESSED: NONE");
				
			}
			
			DBUtil.commit(em);
			
			if (verbose) LogUtil.info(this, "OK");
			
		} catch (Exception ex) {
			if (verbose) LogUtil.info(this, "EXC");
			DBUtil.rollback(em);
		} finally {
			if (verbose) LogUtil.info(this, "FINAL");
			DBUtil.close(em);
		}
	}


}
