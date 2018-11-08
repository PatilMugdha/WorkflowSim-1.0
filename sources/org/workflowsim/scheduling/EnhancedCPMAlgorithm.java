
package org.workflowsim.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.planning.BasePlanningAlgorithm;
import org.workflowsim.utils.HelperFunctions;
import org.workflowsim.utils.Parameters;

public class EnhancedCPMAlgorithm extends BasePlanningAlgorithm {

	private List<Task> statusUpdatedTaskList = new ArrayList<Task>();
	private Map<Task, Double> transferCosts = new HashMap<Task, Double>();
	private Map<Task, Map<CondorVM, Double>> computationCosts = new HashMap<Task, Map<CondorVM, Double>>();
	private double averageBandwidth;
	private double totalCost;
	private Map<Integer, Double> deadlineMap = new HashMap<Integer, Double>();
	private HashMap<Integer, Boolean> taskFlags = new HashMap<Integer, Boolean>();
	private HashMap<Integer, Double> vmMap;

	@Override
	public void run() throws Exception {

		Log.printLine("Critical path-based algorithm running with " + getTaskList().size() + " tasks.");

		forwardPass();
		backwardPass();
		checkCriticalPath();
		initializeTaskStatus();
		selectVMsByExecutionTime();
		assignVmToTask();
		Log.printLine("Total cost: " + totalCost);

	}

	private void initializeTaskStatus() {
		for (int i = 0; i < getTaskList().size(); i++) {
			Task task = (Task) getTaskList().get(i);
			taskFlags.put(task.getCloudletId(), false);
		}
	}

	private void createDeadlineMap(TreeMap<CondorVM, Double> sortedMap) {
		for (int i = 0; i < getTaskList().size(); i++) {
			Task task = (Task) getTaskList().get(i);
			if (task.isCritical()) {
				deadlineMap.put(task.getDepth(), task.getCloudletLength() / sortedMap.firstEntry().getKey().getMips());
                if(deadlineMap.get(task.getDepth())!=null) {
                	deadlineMap.put(task.getDepth(), task.getCloudletLength() / sortedMap.firstEntry().getKey().getMips());
                }
			}
		}

	}

	private void assignVmToTask() {
		Map<Integer, List<Integer>> depthList = createLists();

		for (int i = 0; i < getTaskList().size(); i++) {
			Task task = (Task) getTaskList().get(i);
			Map<CondorVM, Double> costsVm = computationCosts.get(task);

			// sort Vm and get Vm with min cost
			TreeMap<CondorVM, Double> sortedMap = HelperFunctions.sortMapByValue(costsVm);
			createDeadlineMap(sortedMap);
		}

		for (int i = 0; i < getTaskList().size(); i++) {
			Task task = (Task) getTaskList().get(i);
			Map<CondorVM, Double> costsVm = computationCosts.get(task);

			// sort Vm and get Vm with min cost
			TreeMap<CondorVM, Double> sortedMap = HelperFunctions.sortMapByValue(costsVm);

			allotVMs(sortedMap, depthList);
		}

	}

	private void allotVMs(TreeMap<CondorVM, Double> sortedMap, Map<Integer, List<Integer>> map) {

		for (Map.Entry<Integer, Double> entry : deadlineMap.entrySet()) {
			int depth = entry.getKey();
			double deadline = entry.getValue();
			int index = 0;
			for (int i = 0; i < getTaskList().size(); i++) {
				Task task = (Task) getTaskList().get(i);
				int size = map.get(task.getDepth()).size();
				double weight = deadline;
				double remainingWt = 0;
				if (!task.isCritical() && taskFlags.get(task.getCloudletId()) == false) {
					if (task.getDepth() == depth) {
						CondorVM vm = (CondorVM) sortedMap.keySet().toArray()[index];
						remainingWt = weight - (task.getCloudletLength() / vm.getMips());
						if (remainingWt < 0) {
							index++;
							if (index == size - 1) {
								index = 0;
							}
						}
						vm = (CondorVM) sortedMap.keySet().toArray()[index];
						task.setVmId(vm.getId());
						totalCost += (task.getCloudletLength() / vm.getMips());
						taskFlags.put(task.getCloudletId(), true);
						Log.printLine("task(" + task.getCloudletId() + "): " + task.getVmId() + " cost: "
								+ task.getCloudletLength() / vm.getMips());
					}
				} else if (task.isCritical() && taskFlags.get(task.getCloudletId()) == false) {
					if (task.getDepth() == depth) {
						CondorVM vm = (CondorVM) sortedMap.keySet().toArray()[0];
						task.setVmId(vm.getId());
						totalCost += (task.getCloudletLength() / vm.getMips());
						taskFlags.put(task.getCloudletId(), true);
						Log.printLine("task(" + task.getCloudletId() + "): " + task.getVmId() + " cost: "
								+ (task.getCloudletLength() / vm.getMips()));
					}
				}

			}
		}
	}

	private Map<Integer, List<Integer>> createLists() {

		Iterator it = getTaskList().iterator();
		Map<Integer, List<Integer>> map = new HashMap<Integer, List<Integer>>();
		while (it.hasNext()) {
			Task entry = (Task) it.next();
			if (map.get(entry.getDepth()) == null) {
				List<Integer> depthList = new ArrayList<Integer>();
				depthList.add(entry.getCloudletId());
				map.put(entry.getDepth(), depthList);

			} else {
				map.get(entry.getDepth()).add(entry.getCloudletId());
			}
		}
		return map;
	}

	private void selectVMsByExecutionTime() {

		for (Object taskObject : statusUpdatedTaskList) {
			Task task = (Task) taskObject;
			double taskDuration = 0.0;
			if (task.isCritical()) {
				taskDuration = task.getDuration();
			} else {
				taskDuration = task.getDuration() + task.getSlack();
			}

			Map<CondorVM, Double> costsVm = new HashMap<CondorVM, Double>();

			for (Object vmObject : getVmList()) {
				CondorVM vm = (CondorVM) vmObject;
				if (vm.getNumberOfPes() < task.getNumberOfPes()) {
					costsVm.put(vm, Double.MAX_VALUE);
				} else {

					if ((task.getCloudletLength() / vm.getMips()) <= taskDuration) {
						costsVm.put(vm, task.getCloudletLength() / vm.getMips()); // +
																					// (transferCosts.get(task)*vm.getBw()))
																					// / 2);
					}
				}
			}
			if (costsVm.size() == 0) {
				Log.printLine("No VM matches task(" + task.getCloudletId() + ")");
			} else {
				Log.printLine("Total VMs matching task(" + task.getCloudletId() + ") are: " + costsVm.size());
				TreeMap<CondorVM, Double> sorted = HelperFunctions.sortMapByValue(costsVm);
				for (Map.Entry<CondorVM, Double> entry : sorted.entrySet()) {
					System.out.println(entry.getKey().getId());
				}
			}
			computationCosts.put(task, costsVm);
		}
	}

	private void checkCriticalPath() {
		int criticalTasks = 0;
		int nonCriticalTasks = 0;

		for (Object taskObject : getTaskList()) {
			Task task = (Task) taskObject;
			task.setSlack(task.getLateFinishTime() - task.getEarlyFinishTime());
			if (task.getSlack() == 0) {
				task.setCritical(true);
				criticalTasks++;
			} else {
				task.setCritical(false);
				nonCriticalTasks++;
			}
			statusUpdatedTaskList.add(task);
		}
		Log.printLine("CriticalTasks: " + criticalTasks + " NonCriticalTasks: " + nonCriticalTasks);
	}

	private void backwardPass() {
		int i = 0, j = 0;
		for (i = getTaskList().size() - 1; i >= 0; i--) {
			Task task = (Task) getTaskList().get(i);

			if (task.getChildList().size() == 0) {
				task.setLateFinishTime(task.getEarlyFinishTime());
				task.setLateStartTime(task.getLateFinishTime() - task.getDuration());
			} else {
				double min = Double.MAX_VALUE;
				for (j = 0; j < task.getChildList().size(); j++) {

					if (task.getChildList().get(j).getLateStartTime() < min) {
						min = task.getChildList().get(j).getLateStartTime();
					}
				}
				task.setLateFinishTime(min);
				task.setLateStartTime(task.getLateFinishTime() - task.getDuration());
			}
		}
	}

	private void forwardPass() {
		for (Object taskObject : getTaskList()) {
			Task task = (Task) taskObject;
			if (task.getParentList().size() == 0) {
				task.setEarlyStartTime(0);
				task.setEarlyFinishTime(task.getDuration());
			} else {
				double max = Integer.MIN_VALUE;
				for (int i = 0; i < task.getParentList().size(); i++) {
					if (task.getParentList().get(i).getEarlyFinishTime() > max) {
						max = task.getParentList().get(i).getEarlyFinishTime();
					}
				}
				task.setEarlyStartTime(max);
				task.setEarlyFinishTime(task.getEarlyStartTime() + task.getDuration());
			}
		}
	}
}
