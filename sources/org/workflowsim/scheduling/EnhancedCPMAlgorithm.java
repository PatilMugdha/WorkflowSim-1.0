
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

	@Override
	public void run() throws Exception {

		Log.printLine("Critical path-based algorithm running with " + getTaskList().size() + " tasks.");

		averageBandwidth = calculateAverageBandwidth();
		forwardPass();
		backwardPass();
		checkCriticalPath();
		calculateTransferCosts();
		selectVMsByExecutionTime();
		assignVmToTask();
		Log.printLine("Total cost: " + totalCost);

	}

	private void assignVmToTask() {
		Map<Integer, List<Integer>> map = createLists();
		int index = 1;
		for (int i = 0; i < getTaskList().size(); i++) {
			Task task = (Task) getTaskList().get(i);
			Map<CondorVM, Double> costsVm = computationCosts.get(task);

			// sort Vm and get Vm with min cost
			TreeMap<CondorVM, Double> sortedMap = HelperFunctions.sortMapByValue(costsVm);

			if (task.isCritical()) {
				Entry<CondorVM, Double> firstEntry = sortedMap.firstEntry();
				task.setVmId(firstEntry.getKey().getId());
				totalCost += firstEntry.getValue();
				System.out.println("task: " + task.getCloudletId() + " is critical, assigned vm: " + task.getVmId());
			} else {
				// new
				int size = map.get(task.getDepth()).size();

				CondorVM vm = (CondorVM) sortedMap.keySet().toArray()[index];
				index++;
				if (index == size) {
					index = 1;
				}
				System.out.println("VM: " + vm.getId() + " to task: " + task.getCloudletId() + " at index: " + index);

				task.setVmId(vm.getId());
				totalCost += costsVm.get(vm);
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
						costsVm.put(vm, task.getCloudletTotalLength() / vm.getMips()); // +
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
			// System.out.println("LF(" + task.getCloudletId() + "): " +
			// task.getCloudletLength() + " "
			// + task.getLateFinishTime() + " " + task.getLateStartTime());
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

	/**
	 * Populates the transferCosts map with the time in seconds to transfer all
	 * files from each parent to each child
	 */
	private void calculateTransferCosts() {

		for (Object taskObject1 : getTaskList()) {
			Task task1 = (Task) taskObject1;
			Map<Task, Double> taskTransferCosts = new HashMap<Task, Double>();

			transferCosts.put(task1, 0.0);
		}

		// Calculating the actual values
		double totalCost = 0;
		for (Object parentObject : getTaskList()) {
			Task parent = (Task) parentObject;
			for (Task child : parent.getChildList()) {
				totalCost += calculateTransferCost(parent, child);

			}
			transferCosts.put(parent, totalCost);
		}
	}

	/**
	 * Accounts the time in seconds necessary to transfer all files described
	 * between parent and child
	 *
	 * @param parent
	 * @param child
	 * @return Transfer cost in seconds
	 */
	private double calculateTransferCost(Task parent, Task child) {
		List<File> parentFiles = (List<File>) parent.getFileList();
		List<File> childFiles = (List<File>) child.getFileList();

		double acc = 0.0;

		for (File parentFile : parentFiles) {
			if (parentFile.getType() != Parameters.FileType.OUTPUT.value) {
				continue;
			}

			for (File childFile : childFiles) {
				if (childFile.getType() == Parameters.FileType.INPUT.value
						&& childFile.getName().equals(parentFile.getName())) {
					acc += childFile.getSize();
					break;
				}
			}
		}

		// file Size is in Bytes, acc in MB
		acc = acc / Consts.MILLION;
		// acc in MB, averageBandwidth in Mb/s
		return acc * 8 / averageBandwidth;
	}

	private double calculateAverageBandwidth() {
		double avg = 0.0;
		for (Object vmObject : getVmList()) {
			CondorVM vm = (CondorVM) vmObject;
			avg += vm.getBw();
		}
		return avg / getVmList().size();
	}
}
