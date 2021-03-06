package com.linkedin.thirdeye.datalayer.pojo;

import java.util.List;

/**
 * The configuration for a classification job.
 * The classification is supposed to determine the issue type of the anomalies that are generated by the main anomaly
 * function, which is specified by mainFunctionId. Moreover, this configuration provides a list of auxiliary
 * anomaly function ids whose anomalies are used for determining the issue type during the classification.
 */
public class ClassificationConfigBean extends AbstractBean {
  private String name;
  private long mainFunctionId;
  private List<Long> functionIdList;
  private boolean active;

  /**
   * Returns the name of this classification configuration.
   *
   * @return the name of this classification configuration.
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the name of this classification configuration.
   *
   * @param name the name of this classification configuration.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Returns the id of the main function whose anomalies' issue type will be determined.
   *
   * @return the id of the main function whose anomalies' issue type will be determined.
   */
  public long getMainFunctionId() {
    return mainFunctionId;
  }

  /**
   * Sets the id of the main function whose anomalies' issue type will be determined.
   *
   * @param mainFunctionId the id of the main function whose anomalies' issue type will be determined.
   */
  public void setMainFunctionId(long mainFunctionId) {
    this.mainFunctionId = mainFunctionId;
  }

  /**
   * Returns the list of ids of auxiliary anomaly functions whose anomalies are used for determining the issue type of
   * the anomalies from main anomaly function.
   *
   * @return the list of ids of auxiliary anomaly functions.
   */
  public List<Long> getFunctionIdList() {
    return functionIdList;
  }

  /**
   * Sets the list of ids of auxiliary anomaly functions whose anomalies are used for determining the issue type of
   * the anomalies from main anomaly function.
   *
   * @param functionIdList the list of ids of auxiliary anomaly functions.
   */
  public void setFunctionIdList(List<Long> functionIdList) {
    this.functionIdList = functionIdList;
  }

  /**
   * Returns if this configuration is activated.
   *
   * @return if this configuration is activated.
   */
  public boolean isActive() {
    return active;
  }

  /**
   * Sets if this configuration is activated.
   *
   * @param active if this configuration is activated.
   */
  public void setActive(boolean active) {
    this.active = active;
  }
}
