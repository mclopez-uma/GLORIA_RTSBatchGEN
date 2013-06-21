package eu.gloria.rt.worker.offshore;

import java.lang.reflect.Constructor;
import java.nio.channels.ClosedByInterruptException;

import javax.persistence.EntityManager;

import eu.gloria.rt.db.scheduler.ObservingPlan;
import eu.gloria.rt.db.scheduler.ObservingPlanManager;
import eu.gloria.rt.db.util.DBUtil;
import eu.gloria.rt.worker.core.Worker;
import eu.gloria.rti.sch.core.OffshorePluginRetriever;
import eu.gloria.tools.log.LogUtil;

public class WorkerOffshoreRetriever extends Worker{

	@Override
	protected void doAction() throws InterruptedException,
			ClosedByInterruptException, Exception {
		
		
		boolean verbose = true;
		
		EntityManager em = DBUtil.getEntityManager();
		
		ObservingPlan op = null;
		ObservingPlanManager manager = new ObservingPlanManager();
		
		//Build the plugin
		String provider = getPropertyStringValue("ProviderOffshorePluginRetriever");
		if (provider == null) throw new Exception("The ProviderOffshorePluginRetriever key does not exist into the task properties list.");
		Class<?> cls = Class.forName(provider);
		Constructor<?> ct = cls.getConstructor();
		OffshorePluginRetriever retriever = (OffshorePluginRetriever) ct.newInstance();
		retriever.init(this.getProperties());
		//OffshoreRetriever retriever = new OffshoreRetrieverACP();
		
		try{
			
			DBUtil.beginTransaction(em);
			
			op = manager.getNextToProcessFromOffshore(em);
			
			if (op != null){
				
				retriever.retrieve(op.getId());
				
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
