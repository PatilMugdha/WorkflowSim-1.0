package org.workflowsim.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.workflowsim.CondorVM;
//import org.workflowsim.scheduling.CriticalPathBasedSchedulingAlgorithm.ValueComparator;

public class HelperFunctions {

 /*   public static TreeMap<Integer, Double> sortVmByMips(Map<Integer, Double> hm) 
    { 
		Comparator<Double> comparator = new MipsComparator(hm);
		// TreeMap is a map sorted by its keys.
		// The comparator is used to sort the TreeMap by keys.
		TreeMap<Integer, Double> result = new TreeMap<Integer, Double>();

		result.putAll(hm);
		return result;
    } 
    
    
	public static class MipsComparator implements Comparator<Double> {

		HashMap<Integer, Double> map = new HashMap<Integer, Double>();

		public MipsComparator(Map<Integer, Double> Vm) {
			this.map.putAll(Vm);
		}

		@Override
		public int compare(Double o1, Double o2) {
			if (map.get(o1) >= map.get(o2)) {
				return -1;
			} else {
				return 1;
			}
		}
	}*/
    
    
	
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
