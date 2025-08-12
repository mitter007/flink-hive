package com.spdbccc;

import java.util.ArrayList;

/**
 * ClassName: Test
 * Package: com.spdbccc
 * Description:
 *
 * @Author JWT
 * @Create 2025/8/8 15:18
 * @Version 1.0
 */
public class Test {
    public static void main(String[] args) {
        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(3);
        list.add(4);
        list.add(5);
        list.add(6);
        list.removeIf( x->x>3);
        for (Integer i : list) {
            System.out.println(i);
        }



    }
}
