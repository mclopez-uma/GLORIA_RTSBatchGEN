package eu.gloria.rt.worker.offshore.gen;


import javax.persistence.EntityManager;

import eu.gloria.rt.db.scheduler.ObservingPlan;
import eu.gloria.rt.db.scheduler.ObservingPlanManager;
import eu.gloria.rt.db.scheduler.ObservingPlanState;
import eu.gloria.rt.db.util.DBUtil;
import eu.gloria.rt.exception.RTSchException;
import eu.gloria.rti.sch.core.OffshorePluginPublisher;
import eu.gloria.rti.sch.core.OffshorePublisher;

public class OffshorePublisherGEN extends OffshorePluginPublisher implements OffshorePublisher{

	@Override
	public void publish(long idOp) throws RTSchException {
		
		String xmlPath= getPropertyValueString("xmlPath");
		String opXSD = getPropertyValueString("opXSD");
		
				
		
		
		EntityManager em = DBUtil.getEntityManager();
		ObservingPlanManager manager = new ObservingPlanManager();
		
		ObservingPlan dbOp = null;
		
		try{
			
			DBUtil.beginTransaction(em);
			
			dbOp = manager.get(em, idOp);
			
			if (dbOp != null){
				
				try{
					//Recover the OP
					eu.gloria.rti.sch.core.ObservingPlan op = new eu.gloria.rti.sch.core.ObservingPlan(xmlPath + dbOp.getFile() , opXSD);
					
					//TODO
					//Transmit information to the local scheduler
					
					
					dbOp.setState(ObservingPlanState.OFFSHORE);
				
				}catch(Exception ex){
					
					ex.printStackTrace();
					
					dbOp.setState(ObservingPlanState.ERROR);
					dbOp.setComment("ERROR: " + ex.getMessage());
				}
				
			}else{
				
				throw new Exception("OffshorePublisherACP. The observing plan does not exist. ID=" + idOp);
				
			}
			
			DBUtil.commit(em);
			
		} catch (Exception ex) {
			
			DBUtil.rollback(em);
			throw new RTSchException(ex.getMessage());
			
		} finally {
			DBUtil.close(em);
		}
		
		
	}	
	

}
