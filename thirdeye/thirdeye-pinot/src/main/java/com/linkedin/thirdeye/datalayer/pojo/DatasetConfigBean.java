package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.completeness.checker.DataCompletenessConstants.DataCompletenessAlgorithmName;
import com.linkedin.thirdeye.completeness.checker.Wo4WAvgDataCompletenessAlgorithm;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@JsonIgnoreProperties(ignoreUnknown=true)
public class DatasetConfigBean extends AbstractBean {

  public static String DEFAULT_PREAGGREGATED_DIMENSION_VALUE = "all";
  public static String DATASET_OFFLINE_PREFIX = "_OFFLINE";
  public static TimeGranularity DEFAULT_HOURLY_EXPECTED_DELAY = new TimeGranularity(8, TimeUnit.HOURS);
  public static TimeGranularity DEFAULT_DAILY_EXPECTED_DELAY = new TimeGranularity(36, TimeUnit.HOURS);

  private String dataset;

  private List<String> dimensions;

  private String timeColumn;

  private TimeUnit timeUnit;

  private Integer timeDuration;

  private String timeFormat = TimeSpec.SINCE_EPOCH_FORMAT;

  private String timezone = TimeSpec.DEFAULT_TIMEZONE;

  /** Introduce this as a dataset property because count* queries will have no metric information **/
  private String client = PinotThirdEyeClient.CLIENT_NAME;

  private boolean metricAsDimension = false;

  private String metricNamesColumn;

  private String metricValuesColumn;

  /** Autodiscover metrics in case of metricAsDimension */
  private boolean autoDiscoverMetrics = false;

  private boolean active = true;

  /** Configuration for non-additive dataset **/
  // By default, every dataset is additive and this section of configurations should be ignored.
  private boolean additive = true;

  // We assume that non-additive dataset has a default TOP dimension value, which is specified via preAggregatedKeyword,
  // for each dimension. When ThirdEye constructs query string to backend database, it automatically appends the keyword
  // for ALL dimensions except the dimension that has been specified by filter the one specified in
  // dimensionsHaveNoPreAggregation (because some dimension may not have such pre-aggregated dimension value).
  // For example, assume that we have three dimensions: D1, D2, D3. The preAggregatedKeyword is "all" and D2 does not
  // has any pre-aggregated dimension value, i.e., dimensionsHaveNoPreAggregation=[D2]. The filter given by user is
  // {D3=V3}. Then the query should append the WHERE condition {D1=all, D2=V3} in order to get the correct results from
  // this non-additive dataset.
  private List<String> dimensionsHaveNoPreAggregation = Collections.emptyList();

  // The pre-aggregated keyword
  private String preAggregatedKeyword = DEFAULT_PREAGGREGATED_DIMENSION_VALUE;

  // the actual time duration for non-additive dataset
  private Integer nonAdditiveBucketSize;

  // the actual time unit for non-additive dataset
  private TimeUnit nonAdditiveBucketUnit;
  /** End of Configuration for non-additive dataset **/

  private boolean realtime = false;

  private boolean requiresCompletenessCheck = false;
  // delay expected for a dataset for data to arrive
  private TimeGranularity expectedDelay = DEFAULT_DAILY_EXPECTED_DELAY;
  // algorithm to use for computing data completeness
  private DataCompletenessAlgorithmName dataCompletenessAlgorithmName = DataCompletenessAlgorithmName.WO4W_AVERAGE;
  // expected percentage completeness for dataset to be marked complete
  private double expectedCompleteness = Wo4WAvgDataCompletenessAlgorithm.DEFAULT_EXPECTED_COMPLETENESS;


  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<String> dimensions) {
    this.dimensions = dimensions;
  }

  public String getTimeColumn() {
    return timeColumn;
  }

  public void setTimeColumn(String timeColumn) {
    this.timeColumn = timeColumn;
  }

  /**
   * Use DatasetConfigDTO.bucketTimeGranularity instead of this method for considering the additives of the dataset.
   *
   * This method is preserved for reading object from database via object mapping (i.e., Java reflection)
   *
   * @return the time unit of the granularity of the timestamp of each data point.
   */
  @Deprecated
  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
  }

  /**
   * Use DatasetConfigDTO.bucketTimeGranularity instead of this method for considering the additives of the dataset.
   *
   * This method is preserved for reading object from database via object mapping (i.e., Java reflection)
   *
   * @return the duration of the granularity of the timestamp of each data point.
   */
  @Deprecated
  public Integer getTimeDuration() {
    return timeDuration;
  }

  public void setTimeDuration(Integer timeDuration) {
    this.timeDuration = timeDuration;
  }

  public String getTimeFormat() {
    return timeFormat;
  }

  public void setTimeFormat(String timeFormat) {
    this.timeFormat = timeFormat;
  }

  public String getTimezone() {
    return timezone;
  }

  public void setTimezone(String timezone) {
    this.timezone = timezone;
  }

  public String getClient() {
    return client;
  }

  public void setClient(String client) {
    this.client = client;
  }

  public boolean isMetricAsDimension() {
    return metricAsDimension;
  }

  public void setMetricAsDimension(boolean metricAsDimension) {
    this.metricAsDimension = metricAsDimension;
  }

  public String getMetricNamesColumn() {
    return metricNamesColumn;
  }

  public void setMetricNamesColumn(String metricNamesColumn) {
    this.metricNamesColumn = metricNamesColumn;
  }

  public String getMetricValuesColumn() {
    return metricValuesColumn;
  }

  public void setMetricValuesColumn(String metricValuesColumn) {
    this.metricValuesColumn = metricValuesColumn;
  }


  public boolean isAutoDiscoverMetrics() {
    return autoDiscoverMetrics;
  }

  public void setAutoDiscoverMetrics(boolean autoDiscoverMetrics) {
    this.autoDiscoverMetrics = autoDiscoverMetrics;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public boolean isAdditive() {
    return additive;
  }

  public void setAdditive(boolean additive) {
    this.additive = additive;
  }

  public List<String> getDimensionsHaveNoPreAggregation() {
    return dimensionsHaveNoPreAggregation;
  }

  public void setDimensionsHaveNoPreAggregation(List<String> dimensionsHaveNoPreAggregation) {
    this.dimensionsHaveNoPreAggregation = dimensionsHaveNoPreAggregation;
  }

  public String getPreAggregatedKeyword() {
    return preAggregatedKeyword;
  }

  public void setPreAggregatedKeyword(String preAggregatedKeyword) {
    this.preAggregatedKeyword = preAggregatedKeyword;
  }

  public Integer getNonAdditiveBucketSize() {
    return nonAdditiveBucketSize;
  }

  public void setNonAdditiveBucketSize(Integer nonAdditiveBucketSize) {
    this.nonAdditiveBucketSize = nonAdditiveBucketSize;
  }

  public TimeUnit getNonAdditiveBucketUnit() {
    return nonAdditiveBucketUnit;
  }

  public void setNonAdditiveBucketUnit(TimeUnit nonAdditiveBucketUnit) {
    this.nonAdditiveBucketUnit = nonAdditiveBucketUnit;
  }

  public boolean isRealtime() {
    return realtime;
  }

  public void setRealtime(boolean realtime) {
    this.realtime = realtime;
  }

  public boolean isRequiresCompletenessCheck() {
    return requiresCompletenessCheck;
  }

  public void setRequiresCompletenessCheck(boolean requiresCompletenessCheck) {
    this.requiresCompletenessCheck = requiresCompletenessCheck;
  }


  public TimeGranularity getExpectedDelay() {
    return expectedDelay;
  }

  public void setExpectedDelay(TimeGranularity expectedDelay) {
    this.expectedDelay = expectedDelay;
  }


  public double getExpectedCompleteness() {
    return expectedCompleteness;
  }

  public void setExpectedCompleteness(double expectedCompleteness) {
    this.expectedCompleteness = expectedCompleteness;
  }



  public DataCompletenessAlgorithmName getDataCompletenessAlgorithmName() {
    return dataCompletenessAlgorithmName;
  }

  public void setDataCompletenessAlgorithmName(
      DataCompletenessAlgorithmName dataCompletenessAlgorithmName) {
    this.dataCompletenessAlgorithmName = dataCompletenessAlgorithmName;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DatasetConfigBean)) {
      return false;
    }
    DatasetConfigBean dc = (DatasetConfigBean) o;
    return Objects.equals(getId(), dc.getId())
        && Objects.equals(dataset, dc.getDataset())
        && Objects.equals(dimensions, dc.getDimensions())
        && Objects.equals(timeColumn, dc.getTimeColumn())
        && Objects.equals(timeUnit, dc.getTimeUnit())
        && Objects.equals(timeDuration, dc.getTimeDuration())
        && Objects.equals(timeFormat, dc.getTimeFormat())
        && Objects.equals(timezone, dc.getTimezone())
        && Objects.equals(client, dc.getClient())
        && Objects.equals(metricAsDimension, dc.isMetricAsDimension())
        && Objects.equals(metricNamesColumn, dc.getMetricNamesColumn())
        && Objects.equals(metricValuesColumn, dc.getMetricValuesColumn())
        && Objects.equals(autoDiscoverMetrics, dc.isAutoDiscoverMetrics())
        && Objects.equals(active, dc.isActive())
        && Objects.equals(additive, dc.isAdditive())
        && Objects.equals(dimensionsHaveNoPreAggregation, dc.getDimensionsHaveNoPreAggregation())
        && Objects.equals(preAggregatedKeyword, dc.getPreAggregatedKeyword())
        && Objects.equals(nonAdditiveBucketUnit, dc.getNonAdditiveBucketUnit())
        && Objects.equals(nonAdditiveBucketSize, dc.getNonAdditiveBucketSize())
        && Objects.equals(realtime, dc.isRealtime())
        && Objects.equals(requiresCompletenessCheck, dc.isRequiresCompletenessCheck())
        && Objects.equals(expectedDelay, dc.getExpectedDelay())
        && Objects.equals(dataCompletenessAlgorithmName, dc.getDataCompletenessAlgorithmName())
        && Objects.equals(expectedCompleteness,dc.getExpectedCompleteness());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), dataset, dimensions, timeColumn, timeUnit, timeDuration, timeFormat, timezone, client,
        metricAsDimension, metricNamesColumn, metricValuesColumn, autoDiscoverMetrics, active, additive,
        dimensionsHaveNoPreAggregation, preAggregatedKeyword, nonAdditiveBucketSize, nonAdditiveBucketUnit, realtime,
        requiresCompletenessCheck, expectedDelay, dataCompletenessAlgorithmName, expectedCompleteness);
  }

}
