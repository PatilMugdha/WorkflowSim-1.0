
package org.workflowsim.scheduling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.Task;
import org.workflowsim.planning.BasePlanningAlgorithm;
import org.workflowsim.utils.HelperFunctions;

public class EnhancedCPMAlgorithm extends BasePlanningAlgorithm {

	private Map<Task, Map<CondorVM, Double>> taskVmCostValues = new HashMap<Task, Map<CondorVM, Double>>();
	private double totalCost;
	private Map<Integer, Double> deadlineMap = new HashMap<Integer, Double>();
	TreeMap<CondorVM, Double> sortedMap = new TreeMap<CondorVM, Double>();

	@Override
	public void run() throws Exception {

		Log.printLine("Critical path-based algorithm running with " + getTaskList().size() + " tasks.");
		double startTime = System.currentTimeMillis();
		forwardPass();
		backwardPass();
		checkCriticalPath();
		selectVMsByExecutionTime();
		findDeadlines();
		allotVMs();
		Log.printLine("Total cost: " + totalCost);
		double endTime = System.currentTimeMillis() - startTime;
		Log.printLine("Time reqd: " + endTime);
	}

	private List<Task> filter(int depth) {

		List<Task> siblings = new ArrayList<Task>();
		siblings.addAll(getTaskList());
		CollectionUtils.filter(siblings, new Predicate() {

			@Override
			public boolean evaluate(Object o) {
				return ((Task) o).getDepth() == depth;
			}
		});
		return siblings;
	}

	private void findDeadlines() {
		for (int i = 0; i < getTaskList().size(); i++) {
			Task task = (Task) getTaskList().get(i);
			Map<CondorVM, Double> costsVm = taskVmCostValues.get(task);
			// sort Vm and get Vm with min cost
			sortedMap = HelperFunctions.sortMapByValue(costsVm);
			if (task.isCritical()) {
				double taskCost = task.getCloudletLength() / sortedMap.firstEntry().getKey().getMips();
				deadlineMap.put(task.getDepth(), taskCost);
				if (deadlineMap.get(task.getDepth()) != null) {
					// if there are multiple critical paths, take the maximum deadline
					// as the overall deadline for that depth
					if (deadlineMap.get(task.getDepth()) < taskCost) {
						deadlineMap.put(task.getDepth(), taskCost);
					}
				}
			}
		}
	}

	private void allotVMs() {
		// Time-complexity = O(maximum_depth*k) where k=number of siblings at depth i
		//Map<Integer, List<Integer>> depthList = groupTasksAtSameDepth();
		for (Map.Entry<Integer, Double> entry : deadlineMap.entrySet()) {
			int depth = entry.getKey();
			double deadline = entry.getValue();
			int index = 0;
			List<Task> siblings = filter(depth);
			int size = siblings.size();//depthList.get(depth).size(); // total tasks in the same depth
			for (int i = 0; i < size; i++) {
				Task task = (Task) siblings.get(i);
				double weight = deadline;
				double remainingWt = 0;
				CondorVM vm = null;
				Map<CondorVM, Double> costsVm = taskVmCostValues.get(task);
				// sort Vm and get Vm with min cost
				sortedMap = HelperFunctions.sortMapByValue(costsVm);
				if (!task.isCritical()) {
					vm = (CondorVM) sortedMap.keySet().toArray()[index];
					remainingWt = weight - (task.getCloudletLength() / vm.getMips());
					if (remainingWt < 0) {
						index++;
						if (index >= sortedMap.size() - 1) {
							index = 0;
						}
						// re-select VM with new index
						vm = (CondorVM) sortedMap.keySet().toArray()[index];
					}
					task.setVmId(vm.getId());
				} else if (task.isCritical()) {
					vm = (CondorVM) sortedMap.keySet().toArray()[0];
				}
				task.setVmId(vm.getId());
				totalCost += (task.getCloudletLength() / vm.getMips());
				Log.printLine("task(" + task.getCloudletId() + "): " + task.getVmId() + " cost: "
						+ (task.getCloudletLength() / vm.getMips()));
			}
		}
	}

/*	private Map<Integer, List<Integer>> groupTasksAtSameDepth() {

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
	}*/

	private void selectVMsByExecutionTime() {

		for (Object taskObject : getTaskList()) {
			Task task = (Task) taskObject;
			double taskDuration = 0.0;
			if (task.isCritical()) {
				taskDuration = task.getDuration();
			} else {
				taskDuration = task.getDuration() + task.getSlack();
			}
			Log.printLine("Dur for task"+task.getCloudletId()+" "+taskDuration);
			Map<CondorVM, Double> costsVm = new HashMap<CondorVM, Double>();

			for (Object vmObject : getVmList()) {
				CondorVM vm = (CondorVM) vmObject;
				if (vm.getNumberOfPes() < task.getNumberOfPes()) {
					costsVm.put(vm, Double.MAX_VALUE);
				} else {

					if ((task.getCloudletLength() / vm.getMips()) <= taskDuration) {
						costsVm.put(vm, task.getCloudletLength() / vm.getMips());
					}else {
						
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
			taskVmCostValues.put(task, costsVm);
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
