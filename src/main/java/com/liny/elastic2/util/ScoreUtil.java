package com.liny.elastic2.util;

public class ScoreUtil {

	private static int min(int one, int two, int three) {
		int min = one;
		if (two < min) {
			min = two;
		}
		if (three < min) {
			min = three;
		}
		return min;
	}

	public static int ld(String str1, String str2) {
		int d[][]; // 矩阵
		int n = str1.length();
		int m = str2.length();
		int i; // 遍历str1的
		int j; // 遍历str2的
		char ch1; // str1的
		char ch2; // str2的
		int temp; // 记录相同字符,在某个矩阵位置值的增量,不是0就是1
		if (n == 0) {
			return m;
		}
		if (m == 0) {
			return n;
		}
		d = new int[n + 1][m + 1];
		for (i = 0; i <= n; i++) { // 初始化第一列
			d[i][0] = i;
		}
		for (j = 0; j <= m; j++) { // 初始化第一行
			d[0][j] = j;
		}
		for (i = 1; i <= n; i++) { // 遍历str1
			ch1 = str1.charAt(i - 1);
			// 去匹配str2
			for (j = 1; j <= m; j++) {
				ch2 = str2.charAt(j - 1);
				if (ch1 == ch2) {
					temp = 0;
				} else {
					temp = 1;
				}
				// 左边+1,上边+1, 左上角+temp取最小
				d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1]
						+ temp);
			}
		}
		return d[n][m];
	}

	public static double sim(String str1, String str2) {
		if (str2.contains(str1))
			return 1.0f;
		int ld = ld(str1, str2);
		return 1 - (double) ld / Math.max(str1.length(), str2.length());
	}

	public static double getDistance(String target, String other) {
		if (other.contains(target))
			return 1.0;
		char[] sa;
		int n;
		int p[];
		// 'previous' cost array, horizontally
		int d[];
		// cost array, horizontally
		int _d[];
		// placeholder to assist in swapping p and d

		sa = target.toCharArray();
		n = sa.length;
		p = new int[n + 1];
		d = new int[n + 1];

		final int m = other.length();
		if (n == 0 || m == 0) {
			if (n == m) {
				return 1;
			} else {
				return 0;
			}
		}

		// indexes into strings s and t
		int i;
		// iterates through s
		int j;
		// iterates through t

		char t_j;
		// jth character of t

		int cost;
		// cost

		for (i = 0; i <= n; i++) {
			p[i] = i;
		}

		for (j = 1; j <= m; j++) {
			t_j = other.charAt(j - 1);
			d[0] = j;

			for (i = 1; i <= n; i++) {
				cost = sa[i - 1] == t_j ? 0 : 1;
				// minimum of cell to the left+1, to the top+1, diagonally left
				// and up +cost
				d[i] = Math.min(Math.min(d[i - 1] + 1, p[i] + 1), p[i - 1]
						+ cost);
			}
			// copy current distance counts to 'previous row' distance counts
			_d = p;
			p = d;
			d = _d;
		}

		// our last action in the above loop was to switch d and p, so p now
		// actually has the most recent cost counts
		return 1 - ((double) p[n] / Math.max(other.length(), sa.length));
	}

	public static void main(String[] args) {
		String str1 = "贾志忠";
		String str2 = "贾志aaa";
		System.out.println(getDistance(str1, str2));
		System.out.println(sim(str1, str2));

		for (int i = 0; i < 10; i++) {
			getDistance(str1, str2);
		}
	}

}
