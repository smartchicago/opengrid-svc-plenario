/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.opengrid.util;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 *
 * @author Tanyc
 */
public class ColorUtil {
    static Map<Integer, String> dictionary = new HashMap<Integer, String>();
    static { // private constructor
        dictionary.put(0, "#A0522D");
        dictionary.put(1, "#CD5C5C");
        dictionary.put(2, "#FF4500");
        dictionary.put(3, "#008B8B");
        dictionary.put(4, "#B8860B");
        dictionary.put(5, "#32CD32");
        dictionary.put(6, "#FFD700");
        dictionary.put(7, "#48D1CC");
        dictionary.put(8, "#87CEEB");
        dictionary.put(9, "#FF69B4");
        dictionary.put(10, "#CD5C5C");
        dictionary.put(11, "#87CEFA");
        dictionary.put(12, "#6495ED");
        dictionary.put(13, "#DC143C");
        dictionary.put(14, "#FF8C00");
        dictionary.put(15, "#C71585");
        dictionary.put(16, "#000000");
        
    }
     public static String GetRandomColor() {
         Random rand = new Random();
         Integer random = rand.nextInt(17);
        return dictionary.get(random);
     }
     
     public static String GetColor(int counter) {
        return dictionary.get(counter % 17);
     }
}
