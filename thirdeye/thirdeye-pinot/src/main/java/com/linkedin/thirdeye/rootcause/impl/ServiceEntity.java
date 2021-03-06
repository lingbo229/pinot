package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


/**
 * ServiceEntity represents a service associated with certain metrics or dimensions. It typically
 * serves as a connecting piece between observed discrepancies between current and baseline metrics
 * and root cause events such as code deployments. The URN namespace is defined as
 * 'thirdeye:service:{name}'.
 */
public class ServiceEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:service:");

  private final String name;

  protected ServiceEntity(String urn, double score, String name) {
    super(urn, score);
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public ServiceEntity withScore(double score) {
    return new ServiceEntity(this.getUrn(), score, this.name);
  }

  public static ServiceEntity fromName(double score, String name) {
    String urn = TYPE.formatURN(name);
    return new ServiceEntity(urn, score, name);
  }

  public static ServiceEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    String[] parts = urn.split(":", 3);
    if(parts.length != 3)
      throw new IllegalArgumentException(String.format("URN must have 3 parts but has '%d'", parts.length));
    return new ServiceEntity(urn, score, parts[2]);
  }
}
