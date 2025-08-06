package com.spdbccc;

import org.junit.Test;

/**
 * ClassName: test01
 * Package: com.spdbccc
 * Description:
 *
 * @Author JWT
 * @Create 2025/8/4 21:38
 * @Version 1.0
 */
public class test01 {
    //    二分查找是不是数组是有序的
//    这就是一个二分查找的方法
//    public static void main(String[] args) {
//        int[] a = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
//        System.out.println(sort(a, 4));
//    }

    public static String sort(int[] a, int b) {
        int left = 0;
        int right = a.length - 1;
        while (left < right) {
            int mid = (left + right) / 2;
            if (a[mid] == b) {
                return "success:" + mid;
            } else if (mid < b) {
                left = mid;
            } else {
                right = mid;
            }
        }
        return "false";
    }

    @Test
    public void test01(){
        int[] a = {2, 4, 3, 6, 1, 0, 7, 8, 10,9};
//        System.out.println(sort(a, 4));
        System.out.println();
        int[] mp = mp(a);
        for (int i : mp) {
            System.out.print(i+"\t");
        }

    }
//      这个是冒泡排序
    private static  int[] mp(int[]  a) {
        int n =a.length;
        for (int i = 0; i < a.length-1; i++) {
            for (int j = 0; j < n - i - 1; j++) {
                if (a[j]>a[j+1]){
                    int b = a[j+1];
                    a[j+1] = a[j];
                    a[j] = b;

                }
            }
            }
        return a;
        }
//

}
