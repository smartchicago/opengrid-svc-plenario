/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.opengrid.util;

import com.google.gson.Gson;

/**
 *
 * @author Tanyc
 */
public class JSONUtil {
    private static final Gson gson = new Gson();

  private JSONUtil(){}

  public static boolean isJSONValid(String JSON_STRING) {
      try {
          gson.fromJson(JSON_STRING, Object.class);
          return true;
      } catch(com.google.gson.JsonSyntaxException ex) { 
          return false;
      }
  }
}

