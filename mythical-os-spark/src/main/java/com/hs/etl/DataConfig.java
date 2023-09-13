package com.hs.etl;

import com.google.gson.JsonElement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * @Author ChengJie
 * @Date 2023/5/15
 */
public class DataConfig implements Serializable {

  private String type;

  private JsonElement config;

  private List<EncryptDecryptConfig> algConfig = new ArrayList<>();

  private List<WatermarkConfig> watermarkConfigs = new ArrayList<>();

  public List<WatermarkConfig> getWatermarkConfigs() {
    return watermarkConfigs;
  }

  public void setWatermarkConfigs(List<WatermarkConfig> watermarkConfigs) {
    this.watermarkConfigs = watermarkConfigs;
  }

  public List<EncryptDecryptConfig> getAlgConfig() {
    return algConfig;
  }

  public void setAlgConfig(List<EncryptDecryptConfig> algConfig) {
    this.algConfig = algConfig;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }


  public JsonElement getConfig() {
    return config;
  }

  public void setConfig(JsonElement config) {
    this.config = config;
  }
}
