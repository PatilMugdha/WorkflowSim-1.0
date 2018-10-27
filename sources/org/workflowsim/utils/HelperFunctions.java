package org.workflowsim.utils;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.workflowsim.CondorVM;
//import org.workflowsim.scheduling.CriticalPathBasedSchedulingAlgorithm.ValueComparator;

public class HelperFunctions {

	public static TreeMap<CondorVM, Double> sortMapByValue(Map<CondorVM, Double> costsVm) {
		Comparator<CondorVM> comparator = new ValueComparator(costsVm);
		// TreeMap is a map sorted by its keys.
		// The comparator is used to sort the TreeMap by keys.
		TreeMap<CondorVM, Double> result = new TreeMap<CondorVM, Double>(comparator);

		result.putAll(costsVm);
		return result;
	}

	// a comparator that compares Strings
	public static class ValueComparator implements Comparator<CondorVM> {

		HashMap<CondorVM, Double> map = new HashMap<CondorVM, Double>();

		public ValueComparator(Map<CondorVM, Double> costsVm) {
			this.map.putAll(costsVm);
		}

		@Override
		public int compare(CondorVM s1, CondorVM s2) {
			if (map.get(s1) <= map.get(s2)) {
				return -1;
			} else {
				return 1;
			}
		}
	}
}
