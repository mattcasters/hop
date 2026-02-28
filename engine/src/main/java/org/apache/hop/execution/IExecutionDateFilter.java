package org.apache.hop.execution;

import java.util.Date;

public interface IExecutionDateFilter {
  boolean isChosenDate(Date executionStartDate);
}
