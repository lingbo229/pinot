package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;

public class TestMetricConfigManager extends AbstractManagerTestBase {

  private Long metricConfigId1;
  private Long metricConfigId2;
  private Long derivedMetricConfigId;
  private static String dataset1 = "my dataset1";
  private static String dataset2 = "my dataset2";
  private static String metric1 = "metric1";
  private static String metric2 = "metric2";
  private static String derivedMetric1 = "metric3";

  @BeforeClass
  void beforeClass() {
    super.init();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    super.cleanup();
  }

  @Test
  public void testCreate() {

    MetricConfigDTO metricConfig1 = getTestMetricConfig(dataset1, metric1, null);
    metricConfig1.setActive(false);
    metricConfigId1 = metricConfigDAO.save(metricConfig1);
    Assert.assertNotNull(metricConfigId1);

    metricConfigId2 = metricConfigDAO.save(getTestMetricConfig(dataset2, metric2, null));
    Assert.assertNotNull(metricConfigId2);

    MetricConfigDTO metricConfig3 = getTestMetricConfig(dataset1, derivedMetric1, null);
    metricConfig3.setDerived(true);
    metricConfig3.setDerivedMetricExpression("id"+metricConfigId1+"/id"+metricConfigId2);
    derivedMetricConfigId = metricConfigDAO.save(metricConfig3);
    Assert.assertNotNull(derivedMetricConfigId);


  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFind() {
    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findAll();
    Assert.assertEquals(metricConfigs.size(), 3);

    metricConfigs = metricConfigDAO.findByDataset(dataset1);
    Assert.assertEquals(metricConfigs.size(), 2);

    metricConfigs = metricConfigDAO.findActiveByDataset(dataset1);
    Assert.assertEquals(metricConfigs.size(), 1);

    MetricConfigDTO metricConfig = metricConfigDAO.findByMetricAndDataset(metric1, dataset1);
    Assert.assertEquals(metricConfig.getId(), metricConfigId1);

  }

  @Test(dependsOnMethods = { "testFind" })
  public void testFindLike() {
    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findWhereNameLikeAndActive("%m%");
    Assert.assertEquals(metricConfigs.size(), 2);
    metricConfigs = metricConfigDAO.findWhereNameLikeAndActive("%2%");
    Assert.assertEquals(metricConfigs.size(), 1);
    metricConfigs = metricConfigDAO.findWhereNameLikeAndActive("%1%");
    Assert.assertEquals(metricConfigs.size(), 0);
    metricConfigs = metricConfigDAO.findWhereNameLikeAndActive("%p%");
    Assert.assertEquals(metricConfigs.size(), 0);
  }

  @Test(dependsOnMethods = { "testFindLike" })
  public void testUpdate() {
    MetricConfigDTO metricConfig = metricConfigDAO.findById(metricConfigId1);
    Assert.assertNotNull(metricConfig);
    Assert.assertFalse(metricConfig.isInverseMetric());
    metricConfig.setInverseMetric(true);
    metricConfigDAO.update(metricConfig);
    metricConfig = metricConfigDAO.findById(metricConfigId1);
    Assert.assertNotNull(metricConfig);
    Assert.assertTrue(metricConfig.isInverseMetric());
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    metricConfigDAO.deleteById(metricConfigId2);
    MetricConfigDTO metricConfig = metricConfigDAO.findById(metricConfigId2);
    Assert.assertNull(metricConfig);
  }
}
