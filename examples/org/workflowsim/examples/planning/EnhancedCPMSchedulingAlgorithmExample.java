package org.workflowsim.examples.planning;

import java.io.File;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.WorkflowEngine;
import org.workflowsim.WorkflowPlanner;
import org.workflowsim.examples.WorkflowSimBasicExample1;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;

public class EnhancedCPMSchedulingAlgorithmExample extends WorkflowSimBasicExample1{
	   ////////////////////////// STATIC METHODS ///////////////////////
	   public static List<CondorVM> createTenFixedVM(int userId) {

	        //Creates a container to store VMs. This list is passed to the broker later
	        LinkedList<CondorVM> list = new LinkedList<CondorVM>();

	        //VM Parameters
	        long size = 10000; //image size (MB)
	        int ram = 512; //vm memory (MB)
	        int mips = 1000;
	        long bw = 1000;
	        int pesNumber = 1; //number of cpus
	        String vmm = "Xen"; //VMM name

	        //create VMs
	        CondorVM[] vm = new CondorVM[10];
	        
			vm[0] = new CondorVM(0, userId, 120, 1, 2048, (long) 300, size, vmm, 
					new CloudletSchedulerSpaceShared());
			list.add(vm[0]);
			
			vm[1] = new CondorVM(1, userId, 198, 2, 2048, (long) 900, size, vmm, 
					new CloudletSchedulerSpaceShared());
			list.add(vm[1]);
			
			vm[2] = new CondorVM(2, userId, 152, 2, 2048, (long) 850, size, vmm, 
					new CloudletSchedulerSpaceShared());
			list.add(vm[2]);
			
			vm[3] = new CondorVM(3, userId, 444, 4, 2048, (long) 725, size, vmm, 
					new CloudletSchedulerSpaceShared());
			list.add(vm[3]);
			
			vm[4] = new CondorVM(4, userId, 230, 1, 2048, (long) 300, size, vmm, 
					new CloudletSchedulerSpaceShared());
			list.add(vm[4]);
			
			vm[5] = new CondorVM(5, userId, 298, 2, 2048, (long) 350, size, vmm, 
					new CloudletSchedulerSpaceShared());
			list.add(vm[5]);
			
			vm[6] = new CondorVM(6, userId, 120, 4, 2048, (long) 800, size, vmm, 
					new CloudletSchedulerSpaceShared());
			list.add(vm[6]);
			
			vm[7] = new CondorVM(7, userId, 150, 4, 2048, (long) 950, size, vmm, 
					new CloudletSchedulerSpaceShared());
			list.add(vm[7]);
			
			vm[8] = new CondorVM(8, userId, 120, 2, 2048, (long) 750, size, vmm, 
					new CloudletSchedulerSpaceShared());
			list.add(vm[8]);
			
			vm[9] = new CondorVM(9, userId, 110, 1, 2048, (long) 625, size, vmm, 
					new CloudletSchedulerSpaceShared());
			list.add(vm[9]);
	        
	        
	        return list;
	    }
	
	
	
	/**
     * Creates main() to run this example This example has only one datacenter
     * and one storage
     */
    public static void main(String[] args) {


        try {
            // First step: Initialize the WorkflowSim package. 

            /**
             * However, the exact number of vms may not necessarily be vmNum If
             * the data center or the host doesn't have sufficient resources the
             * exact vmNum would be smaller than that. Take care.
             */
            int vmNum = 10;//number of vms;
            /**
             * Should change this based on real physical path
             */
            String daxPath = "C:/Users/patil/git/WorkflowSim-1.0/config/dax/Epigenomics_24.xml";
            
            File daxFile = new File(daxPath);
            if(!daxFile.exists()){
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }

            /**
             * Since we are using HEFT planning algorithm, the scheduling algorithm should be static 
             * such that the scheduler would not override the result of the planner
             */
            Parameters.SchedulingAlgorithm sch_method = Parameters.SchedulingAlgorithm.STATIC;
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.ENHANCED_CPM;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.LOCAL;

            /**
             * No overheads 
             */
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);;
            
            /**
             * No Clustering
             */
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            /**
             * Initialize static parameters
             */
            Parameters.init(vmNum, daxPath, null,
                    null, op, cp, sch_method, pln_method,
                    null, 0);
            ReplicaCatalog.init(file_system);

            // before creating any entities.
            int num_user = 1;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            WorkflowDatacenter datacenter0 = createDatacenter("Datacenter_0");

            /**
             * Create a WorkflowPlanner with one schedulers.
             */
            WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
            /**
             * Create a WorkflowEngine.
             */
            WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
            /**
             * Create a list of VMs.The userId of a vm is basically the id of
             * the scheduler that controls this vm.
             */
            //List<CondorVM> vmlist0 = createVM(wfEngine.getSchedulerId(0), Parameters.getVmNum());
            List<CondorVM> vmlist0 = createTenFixedVM(wfEngine.getSchedulerId(0)); 
            /**
             * Submits this list of vms to this WorkflowEngine.
             */
            wfEngine.submitVmList(vmlist0, 0);

            /**
             * Binds the data centers with the scheduler.
             */
            wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);

            CloudSim.startSimulation();


            List<Job> outputList0 = wfEngine.getJobsReceivedList();

            CloudSim.stopSimulation();

            printJobList(outputList0);


        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }   
}
