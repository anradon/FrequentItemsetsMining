package cmpt741;

import java.util.ArrayList;
//import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

class Entry {
	public Integer first;
	public Integer second;

	Entry() {
	}

	Entry(Integer first, Integer second) {
		this.first = first;
		this.second = second;
	}
}

public class Database {
	public static boolean debugger = false;
	private final List<List<Integer>> transactions;
	private final List<Integer> items;

	public Database(List<List<Integer>> chunk) throws Exception {
		if (debugger) {
			System.out.println("Processing ");
		}
		// int chunkSize = chunk.size();
		transactions = new ArrayList<List<Integer>>(chunk);
		// Collections.copy(transactions,chunk);
		items = new ArrayList<Integer>();
		/*
		 * FileInputStream fin = new FileInputStream(dataFileName);
		 * InputStreamReader istream = new InputStreamReader(fin);
		 * BufferedReader stdin = new BufferedReader(istream); String line;
		 * double startTime = System.currentTimeMillis(); while((line =
		 * stdin.readLine()) != null) { List< Integer > transaction = new
		 * ArrayList< Integer >(); String[] temp = line.split("\\s+");
		 * for(String num : temp) { transaction.add(Integer.parseInt(num)); }
		 * if(transaction.isEmpty()) continue; Collections.sort(transaction);
		 * transactions.add(transaction); } fin.close(); istream.close();
		 * stdin.close();
		 */
		int n = transactions.size();
		int[] header = new int[n];
		PriorityQueue<Entry> pQ = new PriorityQueue<Entry>(n,
				new Comparator<Entry>() {
					public int compare(Entry item1, Entry item2) {
						if (item1.first.equals(item2.first)) {
							return item1.second.compareTo(item2.second);
						} else {
							return item1.first.compareTo(item2.first);
						}
					}
				});
		for (int i = 0; i < n; i++) {
			header[i] = 0;
			pQ.add(new Entry(transactions.get(i).get(header[i]), i));
		}
		while (!pQ.isEmpty()) {
			Entry peek = pQ.remove();
			int val = peek.first;
			int idx = peek.second;
			if (items.isEmpty() || items.get(items.size() - 1) < val) {
				items.add(val);
			}
			while (header[idx] < transactions.get(idx).size()
					&& transactions.get(idx).get(header[idx]) <= val) {
				header[idx]++;
			}
			if (header[idx] < transactions.get(idx).size()) {
				pQ.add(new Entry(transactions.get(idx).get(header[idx]), idx));
			}
		}
		// double endTime = System.currentTimeMillis();
		// System.out.println("Database created in " + (endTime -
		// startTime)/1000.0 + " seconds");
	}

	public int scanDatabase(List<Integer> transaction) {
		int count = 0;
		for (List<Integer> row : transactions) {
			boolean found = true;
			for (Integer item : transaction) {
				int idx, stp, st = 0, en = row.size(), cnt = en - st;
				while (cnt > 0) {
					stp = cnt >> 1;
					idx = st + stp;
					if (row.get(idx).compareTo(item) < 0) {
						st = ++idx;
						cnt -= stp + 1;
					} else {
						cnt = stp;
					}
				}
				if (st == row.size() || row.get(st).compareTo(item) != 0) {
					found = false;
					break;
				}
			}
			if (found)
				count++;
		}
		return count;
	}

	public List<Integer> getItemset() {
		return items;
	}

	public int dbSize() {
		return transactions.size();
	}

//	public List<Integer> getRow(int row) {
//		try {
//			return transactions.get(row);
//		} catch (Exception e) {
//			throw e;
//		}
//	}
}