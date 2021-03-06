package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;


/**
 * EventEntity represents an individual event. It holds meta-data referencing ThirdEye's internal
 * database. The URN namespace is defined as 'thirdeye:event:{type}:{id}'.
 */
public class EventEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:event:");

  private final String type;
  private final long id;

  protected EventEntity(String urn, double score, String type, long id) {
    super(urn, score);
    this.id = id;
    this.type = type;
  }

  public String getType() {
    return type;
  }

  public long getId() {
    return id;
  }

  @Override
  public EventEntity withScore(double score) {
    return new EventEntity(this.getUrn(), score, this.type, this.id);
  }

  public static EventEntity fromDTO(double score, EventDTO dto) {
    String urn = TYPE.formatURN(dto.getEventType(), dto.getId());
    return new EventEntity(urn, score, dto.getEventType(), dto.getId());
  }

  public static EventEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    String[] parts = urn.split(":", 4);
    if(parts.length != 4)
      throw new IllegalArgumentException(String.format("URN must have 3 parts but has '%d'", parts.length));
    return new EventEntity(urn, score, parts[2], Long.parseLong(parts[3]));
  }
}
