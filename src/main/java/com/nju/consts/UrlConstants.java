package com.nju.consts;

import com.nju.wc.JavaWordCount;

/**
 * @description 常量类
 * @date:2022/11/11 9:57
 * @author: qyl
 */
public class UrlConstants {
    public static final String PATH = JavaWordCount.class.getClassLoader().getResource("words.txt").getPath();
}
