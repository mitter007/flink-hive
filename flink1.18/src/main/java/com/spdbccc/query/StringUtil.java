package com.spdbccc.query;


import javax.swing.*;
import java.util.List;

/**
 * ClassName: StringUtil
 * Package: com.spdbccc.hdfs.query
 * Description:
 *
 * @Author 焦文涛
 * @Create 2023/8/16 17:40
 * @Version 1.0
 */
public class StringUtil {

    public static boolean isEmpty(String obj){
        if (obj == null){
            return true;
        }
        if (obj.isEmpty()){
            return true;
        }
        return false;
    }

    public final static boolean isNotNull(String s) { return !isEmpty(s);}
    public final static boolean isTrimEmpty(String s){ return null ==s || s.trim().length()<=0;}

    /**
     * 将对象转换为字符串对象
     * @param input
     *      待取得对象
     * @param defVal
     * 对象为空时的默认返回值
     * @return 取得的字符串对象
     */

    public static String getString(Object input,String defVal){
        if (input == null)
            return defVal;
        String str = input.toString();
        return (str == null)?defVal : str.trim();
    }

    public static String replaceAll(String input,char oldChar,char newChar){
        if (input == null)
            return input;
        int len = input.length();
        if (input.length() == 0)
            return input;
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char ch = input.charAt(i);
            if (ch == oldChar)
                ch= newChar;
            sb.append(ch);
        }
        return sb.toString();
    }
    public static String join(List<String> list,String joinstr){
        String join = String.join(joinstr,list);
        return join;
    }


}
