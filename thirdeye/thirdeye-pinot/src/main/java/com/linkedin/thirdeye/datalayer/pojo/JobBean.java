package com.linkedin.thirdeye.datalayer.pojo;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * This class corresponds to an anomaly job. An anomaly job is created for every execution of an
 * anomaly function spec An anomaly job consists of 1 or more anomaly tasks
 */
public class JobBean extends AbstractBean {

  private String jobName;
  private JobStatus status;
  private TaskType taskType;
  private long scheduleStartTime;
  private long scheduleEndTime;
  private long windowStartTime;
  private long windowEndTime;
  private Timestamp lastModified;
  private long anomalyFunctionId; // 0 if this job is not a detection job

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public JobStatus getStatus() {
    return status;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
  }

  public TaskType getTaskType() {
    return taskType;
  }

  public void setTaskType(TaskType taskType) {
    this.taskType = taskType;
  }

  public long getScheduleStartTime() {
    return scheduleStartTime;
  }

  public void setScheduleStartTime(long scheduleStartTime) {
    this.scheduleStartTime = scheduleStartTime;
  }

  public long getScheduleEndTime() {
    return scheduleEndTime;
  }

  public void setScheduleEndTime(long scheduleEndTime) {
    this.scheduleEndTime = scheduleEndTime;
  }

  public long getWindowStartTime() {
    return windowStartTime;
  }

  public void setWindowStartTime(long windowStartTime) {
    this.windowStartTime = windowStartTime;
  }

  public long getWindowEndTime() {
    return windowEndTime;
  }

  public void setWindowEndTime(long windowEndTime) {
    this.windowEndTime = windowEndTime;
  }

  public Timestamp getLastModified() {
    return lastModified;
  }

  public void setLastModified(Timestamp lastModified) {
    this.lastModified = lastModified;
  }

  public long getAnomalyFunctionId() {
    return anomalyFunctionId;
  }

  public void setAnomalyFunctionId(long anomalyFunctionId) {
    this.anomalyFunctionId = anomalyFunctionId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof JobBean)) {
      return false;
    }
    JobBean af = (JobBean) o;
    return Objects.equals(getId(), af.getId()) && Objects.equals(jobName, af.getJobName()) && Objects
        .equals(status, af.getStatus()) && Objects.equals(scheduleStartTime, af.getScheduleStartTime())
        && Objects.equals(taskType, af.getTaskType()) && Objects.equals(anomalyFunctionId, af.getAnomalyFunctionId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), jobName, status, scheduleStartTime, taskType, anomalyFunctionId);
  }
}
